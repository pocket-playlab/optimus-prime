module OptimusPrime
  module Destinations
    class MergeRecord < Destination
      def initialize(join_keys:)
        @join_keys = join_keys
        @records = {}
      end

      def write(record)
        merge record
      end

      private

      def finish
        push @records.values
      end

      def generate_key(record)
        @join_keys.map { |key| record[key.to_sym] }.join(':')
      end

      def merge(record)
        key = generate_key record
        if @records[key]
          @records[key].merge! record
        else
          @records[key] = record
        end
      end
    end
  end
end
