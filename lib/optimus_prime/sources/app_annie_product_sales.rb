# The AppAnnieProductSales source retrieves the sales data for a single product.
#
# You can pass optional parameters via "options".
# For more details see http://support.appannie.com/hc/en-us/articles/204208914-3-Product-Sales-.

module OptimusPrime
  module Sources
    class AppAnnieProductSales < AppAnnie
      def initialize(api_key:, account_id:, product_id:,
                     start_date:, end_date:, break_down: 'date+country+iap')
        @api_key = api_key
        @path = "/v1.2/accounts/#{account_id}/products/#{product_id}/sales"
        @params = {
          params: { break_down: break_down, start_date: start_date, end_date: end_date }
        }
      end

      def each
        request(method: :get, path: @path, api_key: @api_key, **@params).each do |response|
          yield response
        end
      end

      private

      def request(method:, path:, api_key:, **options)
        Enumerator.new do |enum|
          loop do
            response = super
            enum << response
            break unless response['next_page']

            path = response['next_page']
            options = {}
          end
        end
      end
    end
  end
end
