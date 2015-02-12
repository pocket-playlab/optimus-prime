require 'json'
require 'stringio'
require 'zlib'
require 'aws-sdk'

class EventsCollectorReader < OptimusPrime::Source

  def initialize(bucket:, from:, to:)
    @s3 = Aws::S3::Client.new
    @bucket = bucket
    @from = from
    @to = to
  end

  def each
    objects.each do |object|
      gz = Zlib::GzipReader.new object.body
      gz.each do |line|
        yield JSON.parse line
      end
    end
  end

  private

  def objects
    keys.lazy.map do |key|
      @s3.get_object bucket: @bucket, key: key
    end
  end

  def keys
    pages = @s3.list_objects bucket: @bucket
    keys = pages.flat_map do |page|
      page.contents.map(&:key).select do |key|
        time = Time.parse key
        time >= @from && time < @to
      end
    end
    keys.sort!
  end

end
