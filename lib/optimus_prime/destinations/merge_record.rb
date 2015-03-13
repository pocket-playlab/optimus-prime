module OptimusPrime
  module Destinations
    class MergeRecord < Destination
      attr_reader :join_keys

      def initialize(join_keys:)
        @join_keys = join_keys
        @records = {}
      end

      def write(record)
        merge(record)
      end

      private

      def finish
        push @records.values
      end

      def generate_key(record)
        join_keys.map { |key| record[key.to_sym] }.join(':')
      end

      def merge(record)
        key = generate_key record
        if @records[key].nil?
          @records[key] = record
        else
          @records[key].merge! record
        end
      end
    end
  end
end
