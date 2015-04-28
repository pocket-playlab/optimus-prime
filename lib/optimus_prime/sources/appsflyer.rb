require 'csv'
require 'rest_client'

module OptimusPrime
  module Sources
    class Appsflyer < OptimusPrime::Source
      def initialize(app_id:, api_token:, report_type:, from:, to:)
        @url = "https://hq.appsflyer.com/export/#{app_id}/#{report_type}_report"
        @query = {
          api_token: api_token,
          from: from.to_date,
          to: to.to_date,
        }
      end

      def each
        CSV.parse api_response.force_encoding('ASCII-8BIT'), headers: true do |row|
          yield row.to_h
        end
      end

      private

      def api_response
        RestClient.get @url, params: @query
      end
    end
  end
end