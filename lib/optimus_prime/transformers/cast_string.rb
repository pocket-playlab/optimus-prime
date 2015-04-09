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
      # type_map - Hash specifying the types like in the example above
      # stringify - Optional Boolean you can set to false to
      #             NOT convert Symbol keys of the type_map to Strings
      def initialize(type_map:, stringify: true)
        @type_map = stringify ? type_map.stringify_nested_symbolic_keys : type_map
      end

      def write(record)
        transformed_record = transform(record)
        push transformed_record if transformed_record
      end

      private

      def transform(record)
        # NOTE: Could also use #map here to not overwrite the existing record.
        record.each do |key, val|
          return if val.nil?
          type = @type_map[key.to_s]
          record[key] = case type && type.downcase
            when nil        then val
            when 'string'   then val
            # Using Integer and Float as constructors to raise Exception.
            when 'integer'  then Integer(val, 10)
            when 'float'    then Float(val)
            # NOTE: Every string not being 'true' results in false.
            when 'boolean'  then val.downcase == 'true'
            when 'date'     then Date.parse(val)
            else raise TypeError.new("Cannot convert #{type} to String!")
          end
        end
      rescue => e
        raise e if e.instance_of? TypeError # if the map has a problem, we should blow up
        logger.error("Exception handled - #{e.class}: #{e.message} - record: #{record}")
        false
      end
    end
  end
end
