module OptimusPrime
  module Transformers
    class KeyFilter < Destination
      # This class accepts a hash object and removes pairs
      # whose key is not included in the array given in the
      # initializer.

      def initialize(fields:)
        @fields = fields
      end

      def write(record)
        push transform(record)
      end

      private

      def transform(record)
        record.select { |key, value| @fields.include? key }
      end
    end
  end
end
