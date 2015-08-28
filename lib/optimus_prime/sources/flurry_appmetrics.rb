# Flurry AppMetrics API source
#
# Please see https://developer.yahoo.com/flurry/docs/api/code/appmetrics/ for API implementation.
#
# Paramaters:
#   api_access_code: Flurry API Access Code
#   api_key: Flurry API Key
#   start_date: Start date for the report in '%Y-%m-%d' format
#   end_date: End date for the report in '%Y-%m-%d' format
#   metric_name: Name of metric to gather report for as documented in API documentation linked above.
#   version (optional): Version of app to filter report on. Called versionName in documentation
#   country (optional): Country to filter report on.
#   group_by (optional): Change the grouping of report into DAYS, WEEKS, or MONTHS
#
# If any non-200 response is received an exception will be raised and the error
# logged.

require 'rest_client'
require 'json'

module OptimusPrime
  module Sources
    class FlurryAppMetrics < OptimusPrime::Source
      def initialize(api_access_code:, api_key:, start_date:, end_date:, metric_name:,
                     version: nil, country: nil, group_by: nil)
        @params = {
          apiAccessCode: api_access_code, apiKey: api_key, startDate: start_date,
          endDate: end_date, versionName: version, country: country, groupBy: group_by
        }
        @params.each { |k, v| @params.delete(k) if v.nil? }

        @url = "http://api.flurry.com/appMetrics/#{metric_name}"
        raise ArgumentError.new 'start_date > end_date' if start_date > end_date
      end

      def each
        yield get_report
      end

      private

      def get_report
        handle_response RestClient.get(@url, params: @params)
      end

      def handle_response(response)
        return JSON.parse(response) unless response.code != 200
        msg = "An invalid response was received from Flurry - #{response}"
        logger.error msg
        raise Exception.new(msg)
      end
    end
  end
end
