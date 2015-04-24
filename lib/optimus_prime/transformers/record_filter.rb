module OptimusPrime
  module Transformers
    class RecordFilter < Destination
      def initialize(constraints:)
        @rules = constraints.with_indifferent_access
      end

      def write(record)
        push(record) if valid?(record)
      end

      private

      def valid?(record)
        @rules.all? do |field, rule|
          send(rule['type'], record[field], rule['values'])
        end
      end

      def range(value, params)
        value && value >= params[0] && value <= params[1]
      end

      def less_than_or_equal(value, params)
        value && value <= params[0]
      end

      def less_than(value, params)
        value && value < params[0]
      end

      def greater_than_or_equal(value, params)
        value && value >= params[0]
      end

      def greater_than(value, params)
        value && value > params[0]
      end

      def set(value, params)
        params.include? value
      end

      def not_set(value, params)
        not set(value, params)
      end

      def contained(value, params)
        params.any? { |p| value.include?(p) }
      end

      def not_contained(value, params)
        not contained(value, params)
      end
    end
  end
end
