# The CastString transformer converts
# string values in a Hash to their real types.
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
    class CastString < Destination
      TRUTHY_STRING = /^(true|yes)$/

      # type_map - Hash specifying the types like in the example above
      # stringify - Optional Boolean you can set to false to
      #             NOT convert Symbol keys of the type_map to Strings
      def initialize(type_map: {})
        @type_map = type_map.with_indifferent_access
      end

      def write(record)
        transformed_record = transform(record)
        push transformed_record if transformed_record
      end

      private

      def transform(record)
        record.map do |key, val|
          type = @type_map[key]
          result = val.is_a?(String) ? cast(val, type) : val
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
        when 'string'   then val
        # Using Integer and Float as constructors to raise Exception.
        when 'integer'  then Integer(val, 10)
        when 'float'    then Float(val)
        # NOTE: Every string not being 'true' or 'yes' results in false.
        when 'boolean'  then !(val.downcase =~ TRUTHY_STRING).nil?
        when 'date'     then Date.parse(val)
        else raise TypeError.new("Cannot convert #{type} to String!")
        end
      end
    end
  end
end
