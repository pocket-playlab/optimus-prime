module OptimusPrime
  module Transformers
    class RenameKey < Destination
      # This class accepts a hash object and replaces incorrect
      # keys with correct ones based on the mapping hash given
      # in the initializer.

      def initialize(mapper:)
        @mapper = mapper
      end

      def write(record)
        push transform(record)
      end

      private

      def transform(record)
        record.each_with_object({}) do |(key, value), new_record|
          new_record[@mapper[key] || key] = value
        end
      end
    end
  end
end
