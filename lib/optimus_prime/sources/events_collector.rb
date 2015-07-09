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
            row = parse(line)
            yield row if row
          end
        end
      end

      def parse(line)
        JSON.parse line
      rescue => e
        logger.error "#{e.class} #{e.message} : Cannot parse the line #{line}"
        nil
      end
    end
  end
end
