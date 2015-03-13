require 'optimus_prime'
require 'optimus_prime/destination'

module OptimusPrime
  module Destinations
    class NativeTypeCast < Destination
      # This class accepts a hash object and converts its values
      # to their real data types based on the map of fieldname/
      # datatype given in the initializer.

      def initialize(type_map:)
        @type_map = type_map
        @fields = @type_map.keys # caching for performance
      end

      def write(record)
        transformed_record = transform(record)
        push transformed_record if transformed_record
      end

      private

      def transform(record)
        record.keys.each do |field|
          next unless @fields.include? field
          case @type_map[field].downcase
          when 'integer'
            record[field] = Integer(record[field], 10)
          when 'float'
            record[field] = Float(record[field])
          when 'boolean'
            record[field] = (record[field].downcase == "true")
          when 'string'
            record[field] = record[field].to_s
          when 'hash'
            record[field] = record[field].to_h
          when 'array'
            record[field] = record[field].to_a
          else
            raise TypeError.new("#{@type_map[field]} is not a native data type!")
          end
        end
        record
      rescue => e
        raise e if e.instance_of? TypeError # if the map has a problem, we should blow up
        logger.error("Exception handled - #{e.class}: #{e.message} - record: #{record}")
        return false
      end
    end
  end
end