# The AppAnnieCampaignList source retrieves AppSite and Campaign data from AppAnnie.
#
# You can pass optional parameters via "options".
# For more details see https://support.appannie.com/hc/en-us/articles/204209014-6-App-Site-Campaign-List-

module OptimusPrime
  module Sources
    class AppAnnieCampaignList < AppAnnie
      def initialize(api_key:, market:, product_id:)
        @api_key = api_key
        @path = "/v1.2/apps/#{market}/app/#{product_id}/ad_items"
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
