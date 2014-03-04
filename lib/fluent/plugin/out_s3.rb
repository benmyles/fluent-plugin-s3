module Fluent

require 'fluent/mixin/config_placeholders'

class S3Output < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('s3', self)

  unless method_defined?(:log)
    define_method(:log) { $log }
  end

  def initialize
    super
    require 'aws-sdk'
    require 'zlib'
    require 'time'
    require 'tempfile'
    require 'open3'
    require 'csv'
    require 'bson'

    @use_ssl = true
  end

  config_param :path, :string, :default => ""
  config_param :time_format, :string, :default => nil

  include SetTagKeyMixin
  config_set_default :include_tag_key, false

  include SetTimeKeyMixin
  config_set_default :include_time_key, false

  config_param :aws_key_id, :string, :default => nil
  config_param :aws_sec_key, :string, :default => nil
  config_param :s3_bucket, :string
  config_param :s3_endpoint, :string, :default => nil
  config_param :s3_object_key_format, :string, :default => "%{path}%{time_slice}_%{index}.%{file_extension}"
  config_param :store_as, :string, :default => "gzip"
  config_param :out_format, :string, :default => nil
  config_param :csv_col_sep, :string, :default => "|"
  config_param :csv_sort_cols, :bool, :default => true
  config_param :auto_create_bucket, :bool, :default => true
  config_param :check_apikey_on_start, :bool, :default => true
  config_param :proxy_uri, :string, :default => nil
  config_param :reduced_redundancy, :bool, :default => false

  attr_reader :bucket

  include Fluent::Mixin::ConfigPlaceholders

  def placeholders
    [:percent]
  end

  def configure(conf)
    super

    @out_format = conf['out_format']
    @out_format = 'json' if conf['format_json']

    if csv_sort_cols = conf['csv_sort_cols']
      if csv_sort_cols.empty?
        @csv_sort_cols = true
      else
        @csv_sort_cols = Config.bool_value(csv_sort_cols)
        if @csv_sort_cols.nil?
          raise ConfigError, "'true' or 'false' is required for csv_sort_cols option on s3 output"
        end
      end
    end

    if csv_col_sep = conf['csv_col_sep']
      if csv_col_sep.empty?
        @csv_col_sep = '|'
      else
        @csv_col_sep = csv_col_sep
        if @csv_col_sep.nil?
          raise ConfigError, "string is required for csv_col_sep option on s3 output"
        end
      end
    end

    if use_ssl = conf['use_ssl']
      if use_ssl.empty?
        @use_ssl = true
      else
        @use_ssl = Config.bool_value(use_ssl)
        if @use_ssl.nil?
          raise ConfigError, "'true' or 'false' is required for use_ssl option on s3 output"
        end
      end
    end

    @ext, @mime_type = case @store_as
      when 'gzip' then ['gz', 'application/x-gzip']
      when 'lzo' then
        begin
          Open3.capture3('lzop -V')
        rescue Errno::ENOENT
          raise ConfigError, "'lzop' utility must be in PATH for LZO compression"
        end
        ['lzo', 'application/x-lzop']
      when 'json' then ['json', 'application/json']
      else ['txt', 'text/plain']
    end

    @timef = TimeFormatter.new(@time_format, @localtime)

    if @localtime
      @path_slicer = Proc.new {|path|
        Time.now.strftime(path)
      }
    else
      @path_slicer = Proc.new {|path|
        Time.now.utc.strftime(path)
      }
    end

  end

  def start
    super
    options = {}
    if @aws_key_id && @aws_sec_key
      options[:access_key_id] = @aws_key_id
      options[:secret_access_key] = @aws_sec_key
    end
    options[:s3_endpoint] = @s3_endpoint if @s3_endpoint
    options[:proxy_uri] = @proxy_uri if @proxy_uri
    options[:use_ssl] = @use_ssl

    @s3 = AWS::S3.new(options)
    @bucket = @s3.buckets[@s3_bucket]

    ensure_bucket
    check_apikeys if @check_apikey_on_start
  end

  def format(tag, time, record)
    time_str = if @include_time_key || @out_format.to_s.empty?
      @timef.format(time)
    else
      nil
    end

    # copied from each mixin because current TimeSlicedOutput can't support mixins.
    if @include_tag_key
      record[@tag_key] = tag
    end
    if @include_time_key
      record[@time_key] = time_str
    end

    case @out_format.to_s
    when 'csv' then
      format_csv(tag, time, record)
    else
      format_default(tag, time, record, time_str)
    end
  end

  def write(chunk)
    i = 0

    begin
      path = @path_slicer.call(@path)
      values_for_s3_object_key = {
        "path" => path,
        "time_slice" => chunk.key,
        "file_extension" => @ext,
        "index" => i,
        "bsonid" => BSON::ObjectId.new.to_s
      }
      s3path = @s3_object_key_format.gsub(%r(%{[^}]+})) { |expr|
        values_for_s3_object_key[expr[2...expr.size-1]]
      }
      i += 1
    end while @bucket.objects[s3path].exists?

    tmp = Tempfile.new("s3-")
    begin
      if @store_as == "gzip"
        w = Zlib::GzipWriter.new(tmp)
        chunk.write_to(w)
        w.close
      elsif @store_as == "lzo"
        w = Tempfile.new("chunk-tmp")
        chunk.write_to(w)
        w.close
        tmp.close
        # We don't check the return code because we can't recover lzop failure.
        system "lzop -qf1 -o #{tmp.path} #{w.path}"
      else
        chunk.write_to(tmp)
        tmp.close
      end
      @bucket.objects[s3path].write(Pathname.new(tmp.path), {:content_type => @mime_type,
                                                             :reduced_redundancy => @reduced_redundancy})
    ensure
      tmp.close(true) rescue nil
      w.close rescue nil
      w.unlink rescue nil
    end
  end

  private

  def format_default(tag, time, record, time_str)
    if @out_format == 'json'
      Yajl.dump(record) + "\n"
    else
      "#{time_str}\t#{tag}\t#{Yajl.dump(record)}\n"
    end
  end

  def format_csv(tag, time, record)
    row = if @csv_sort_cols
      record.keys.sort.map { |k| record[k] }
    else
      record.values
    end

    CSV.generate_line(row, col_sep: @csv_col_sep)
  end

  def ensure_bucket
    if !@bucket.exists?
      if @auto_create_bucket
        log.info "Creating bucket #{@s3_bucket} on #{@s3_endpoint}"
        @s3.buckets.create(@s3_bucket)
      else
        raise "The specified bucket does not exist: bucket = #{@s3_bucket}"
      end
    end
  end

  def check_apikeys
    @bucket.empty?
  rescue
    raise "aws_key_id or aws_sec_key is invalid. Please check your configuration"
  end
end


end
