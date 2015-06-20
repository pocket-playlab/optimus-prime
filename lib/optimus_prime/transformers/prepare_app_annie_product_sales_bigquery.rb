module OptimusPrime
  module Transformers
    # This class accepts data from AppAnnieProductSales source
    # that broken down the data by 'date+country+iap'
    # and wrap nested-fields with [] so that sales data can be
    # loaded into Google BigQuery as the RECORD type properly.
    # It also converts the data type of revenue fields that are String to Float.
    #
    # Each hash will contain default fields specified in @fields.
    class PrepareAppAnnieProductSalesBigquery < OptimusPrime::Destination
      def initialize
        @records = []
        @fields = ['vertical', 'currency', 'market', 'sales_list', 'iap_sales_list']
      end

      def write(data)
        prepare data.select { |k, v| @fields.include? k }
      end

      def finish
        @records.each { |record| push record }
      end

      private

      def prepare(data)
        convert_revenue data
        wrap_nested_fields data
        @records << data
      end

      def each_sale(data)
        ['sales_list', 'iap_sales_list'].each do |sale_type|
          data[sale_type].each { |row| yield row }
        end
      end

      def convert_revenue(data)
        each_sale(data) do |row|
          row['revenue'].each do |key, value|
            if value.is_a? Hash
              value.each { |k, v| row['revenue'][key][k] = v.to_f }
            else
              row['revenue'][key] = value.to_f
            end
          end
        end
      end

      def wrap_nested_fields(data)
        each_sale(data) do |row|
          row['revenue'].each do |key, value|
            row['revenue'][key] = [value] if value.is_a? Hash
          end
          row['revenue'] = [row['revenue']]
        end
      end
    end
  end
end
