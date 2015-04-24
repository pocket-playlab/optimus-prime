# The CastType transformer converts values in a Hash to other types.
#
# Keys in the hash can be strings or symbols.
#
# You configure the class with a type_map.
# All other keys are not changed.
#
# Example type_map with all supported types:
#
# {
#   amount: 'integer',
#   price: 'float',
#   is_available: 'boolean',
#   notes: 'string',
#   due: 'date'
# }
#
# With each write it accepts a Hash and and returns a Hash with converted types.

require 'date'

module OptimusPrime
  module Transformers
    class CastType < Destination
      TRUTHY_STRINGS = %w(true yes)

      # type_map - Hash specifying the types like in the example above
      # stringify - Optional Boolean you can set to false to
      #             NOT convert Symbol keys of the type_map to Strings
      def initialize(type_map: {}, stringify: true)
        @type_map = stringify ? type_map.stringify_nested_symbolic_keys : type_map
      end

      def write(record)
        transformed_record = transform(record)
        push transformed_record if transformed_record
      end

      private

      def transform(record)
        record.map do |key, val|
          type = @type_map[key.to_s]
          result = val && cast(val, type)
          [key, result]
        end.to_h
      rescue TypeError
        raise
      rescue => e
        logger.error("Exception handled - #{e.class}: #{e.message} - record: #{record}")
        false
      end

      def cast(val, type)
        case type && type.downcase
        when nil        then val
        # Using String, Integer and Float as constructors to raise exception.
        when 'string'   then String(val)
        when 'integer'  then Integer(val, 10)
        when 'float'    then Float(val)
        when 'boolean'  then TRUTHY_STRINGS.include?(String(val).downcase)
        when 'date'     then Date.parse(val)
        when 'datetime' then val.is_a?(Integer) ? Time.at(val).to_datetime : DateTime.parse(val)
        else raise TypeError.new("Cannot convert #{type}")
        end
      end
    end
  end
end
