# The AppAnnieProductSales source retrieves the sales data for a single product.
#
# You can pass optional parameters via "options".
# For more details see https://support.appannie.com/hc/en-us/articles/204208994-1-Account-Connections-List.

module OptimusPrime
  module Sources
    class AppAnnieAccountConnectionList < AppAnnie
      def initialize(api_key:)
        @api_key = api_key
        @path = "/v1.2/accounts?page_index=0"
        @params = {}
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
