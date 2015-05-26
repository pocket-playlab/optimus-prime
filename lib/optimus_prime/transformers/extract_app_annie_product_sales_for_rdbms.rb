module OptimusPrime
  module Transformers
    # This class accepts data from AppAnnie Product Sales,
    # extracts sales_list and iap_sales_list data, and transforms
    # them into an array of hash. Each hash will contain default fields
    # specified in the "default_fields" variable.
    class ExtractAppAnnieProductSalesForRdbms < OptimusPrime::Destination
      def initialize
        @rows = []
        @default_fields = ['vertical', 'currency', 'market']
      end

      def write(data)
        extract data
      end

      def finish
        @rows.each { |row| push row }
      end

      private

      def extract(data)
        extract_list data, 'sales_list'
        extract_list data, 'iap_sales_list'
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
