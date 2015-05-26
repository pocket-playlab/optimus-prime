module OptimusPrime
  module Transformers
    # This class accepts data from AppAnnie Product Sales,
    # extracts sales_list and iap_sales_list data, and transforms
    # them into an array of hash. Each hash will contain default fields
    # specified in the "default_fields" argument.
    class ExtractAppAnnieProductSalesForRdbms < OptimusPrime::Destination
      def initialize(default_fields:)
        @rows = []
        @default_fields = default_fields
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
          data.each do |key, value|
            if value.is_a? Hash
              value.each { |k, v| new_row["#{prefix}_#{key}_#{k}"] = v }
            else
              new_row["#{prefix}_#{key}"] = value
            end
          end
        else
          new_row["#{prefix}"] = data
        end
        new_row
      end

      def default_fields(data)
        data.select { |k, v| @default_fields.include? k }
      end

      def extract_list(data, prefix)
        data[prefix].each do |row|
          new_row = default_fields data
          row.each { |key, value| new_row.merge! expand_hash(value, "#{prefix}_#{key}") }
          @rows << new_row
        end
      end
    end
  end
end
