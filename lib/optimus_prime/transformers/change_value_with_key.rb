module OptimusPrime
  module Transformers
    # The ChangeValueWithKey transformer changes values of a Hash with specify key in mapper variable
    #
    # to different values based on a mapper.
    #
    # Any type of value is supported.
    # Not matching values stay untouched.
    #
    # Example mapper:
    #
    # {
    #   'key1' => 100,
    #   'key2' => false
    # }
    class ChangeValueWithKey < Destination
      def initialize(mapper:)
        @mapper = mapper.with_indifferent_access
      end

      def write(record)
        push change_value(record)
      end

      private

      def change_value(record)
        record.each do |key, value|
          record[key] = @mapper[key] if @mapper.has_key? key
        end
        record
      end
    end
  end
end
