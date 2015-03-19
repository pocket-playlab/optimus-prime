module OptimusPrime
  module Transformers
    class Validator < Destination
      # This class accepts a hash object and converts its values
      # to their real data types based on the map of fieldname/
      # datatype given in the initializer.

      def initialize(constraints:, logging: true)
        @rules = constraints
        @logging = logging
      end

      def write(record)
        push(record) if valid?(record)
      end

      private

      def valid?(record)
        @rules.each do |field, rule|
          next if send("#{rule[:type]}_validator", record[field], rule[:values])
          logger.error(record) if @logging
          return false
        end
        true
      end

      def range_validator(value, specifieres)
        value >= specifieres[0] && value <= specifieres[1]
      end

      def less_than_or_equal_validator(value, specifieres)
        value <= specifieres[0]
      end

      def less_than_validator(value, specifieres)
        value < specifieres[0]
      end

      def greater_than_or_equal_validator(value, specifieres)
        value >= specifieres[0]
      end

      def greater_than_validator(value, specifieres)
        value > specifieres[0]
      end

      def set_validator(value, specifieres)
        specifieres.include? value
      end
    end
  end
end
