module OptimusPrime
  module Transformers
    class CastString < Destination
      # This class accepts a hash object and converts its values
      # to their real data types based on the map of fieldname/
      # datatype given in the initializer.

      def initialize(type_map:)
        @type_map = type_map
      end

      def write(record)
        transformed_record = transform(record)
        push transformed_record if transformed_record
      end

      private

      def transform(record)
        record.keys.each do |field|
          next unless @type_map.include? field
          record[field] = case @type_map[field].downcase
            when 'integer' then Integer(record[field], 10)
            when 'float'   then Float(record[field])
            when 'boolean' then (record[field].downcase == "true")
            when 'string'  then record[field] # the same
            else raise TypeError.new("Cannot convert #{@type_map[field]} to String!")
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