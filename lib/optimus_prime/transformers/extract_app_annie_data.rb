module OptimusPrime
  module Transformers
    # This class create for flatten nested json from AppAnnie to single hash.
    # This class rewrite from ExtractAppAnnieProductSales to make more flexible for "default_fields" and "extract_keys" variables
      # default_fields: chosen key to assign in each hash after flatten.
      # extract_keys: flatten data inside these key into single hash.
    # See example in spec and PR

    class ExtractAppAnnieData < OptimusPrime::Destination
      def initialize(default_fields:, extract_keys:)
        @rows = []
        @default_fields = default_fields
        @extract_keys = extract_keys
      end

      def write(data)
        extract data
      end

      def finish
        @rows.each { |row| push row }
      end

      private

      def extract(data)
        @extract_keys.each do |key|
          extract_list data, key
        end
      end

      def expand_hash(data, prefix)
        new_row = {}
        if data.is_a? Hash
          data.each { |key, value| set_new_key(new_row, value, "#{prefix}_#{key}") }
        else
          new_row["#{prefix}"] = data
        end
        new_row
      end

      def set_new_key(row, value, key)
        value.is_a?(Hash) ? value.each { |k, v| row["#{key}_#{k}"] = v } : row[key] = value
      end

      def default_fields(data)
        data.select { |k, v| @default_fields.include? k }
      end

      def extract_list(data, prefix)
        data[prefix].each do |row|
          new_row = default_fields(data).merge('sales_type' => prefix)
          row.each { |key, value| new_row.merge! expand_hash(value, key) }
          @rows << new_row
        end
      end
    end
  end
end
