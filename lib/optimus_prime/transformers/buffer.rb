# buffer is not meant to be used for big quantities of data
# and is mostly thought as a simple buffer for collecting data from multiple sources

module OptimusPrime
  module Transformers
    class Buffer < Destination
      def initialize
        @buffer = []
      end

      def write(record)
        @buffer << record
      end

      def finish
        @buffer.each { |record| push record }
      end
    end
  end
end
