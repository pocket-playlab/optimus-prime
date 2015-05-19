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
