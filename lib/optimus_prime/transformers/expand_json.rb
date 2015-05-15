require 'json'

module OptimusPrime
  module Transformers
    # Accepts a Hash object and expands the JSON values of specified fields.
    class ExpandJSON < Destination
      attr_accessor :fields, :overwrite

      def initialize(fields:, overwrite: true)
        @fields = fields
        @overwrite = overwrite
      end

      def write(record)
        transformed_record = transform(record)
        push transformed_record if transformed_record
      end

      private

      def transform(record)
        result = record.dup
        fields.each do |field|
          next unless result.key? field
          result = expand(result, field)
          return false unless result
        end
        result
      end

      def expand(record, field)
        expanded_field = read_value(record, field)
        record.delete field
        overwrite ? record.merge(expanded_field) : expanded_field.merge(record)
      rescue => err
        logger.error error_message(err, record, field)
        return false
      end

      def read_value(record, field)
        JSON.parse(record[field])
      end

      def error_message(err, record, field)
        "#{err.message}: Cannot expand invalid JSON field '#{field}' in: #{record}"
      end

    end
  end
end
