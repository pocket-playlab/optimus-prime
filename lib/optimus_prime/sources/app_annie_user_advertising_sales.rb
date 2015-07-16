# The AppAnnieUserAdvertisingSales source retrieves the user adsvertising sales data from AppAnnie.

# Advertising metrics broken down by ad account, country, app, ad_item and/or date.
# Data can be filtered by country, ad account and app.
# The metrics returned will be based on the data breakdown chosen.
# Breakdown parameter must be provided.

# You can pass optional parameters via "options".
# For more details see https://support.appannie.com/hc/en-us/articles/204208944-8-User-Advertising-Sales.

module OptimusPrime
  module Sources
    class AppAnnieUserAdvertisingSales < AppAnnie
      def initialize(api_key:, break_down:, options: {})
        @api_key = api_key
        url_params = parameterize(options.merge(break_down: break_down))
        @path = "/v1.2/ads/sales?#{url_params}"
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

      def parameterize(params)
        URI.escape(params.collect{|k,v| "#{k}=#{v}"}.join('&'))
      end
    end
  end
end
