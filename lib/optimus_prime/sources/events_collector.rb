require 'json'
require 'stringio'
require 'zlib'
require 'aws-sdk'

module OptimusPrime
  module Sources
    class EventsCollector < OptimusPrime::Sources::S3Source
      def each
        objects.each do |object|
          gz = Zlib::GzipReader.new object.body
          gz.each do |line|
            yield JSON.parse line
          end
        end
      end
    end
  end
end
