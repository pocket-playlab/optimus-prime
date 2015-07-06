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
            begin
              yield JSON.parse line
            rescue Exception => exception
              logger.error "Cannot parse line : #{line}"
              next
            end
          end
        end
      end
    end
  end
end
