# The GeoIP Lookup transformer obtains the geographic information
# for the given IP address.
#
# Geographic information from the freegeoip API will be added to the record
# by appending geo_ for each key from the JSON response.
#
# Parameters:
#   :ip_field - Field in the record containing the IP Address.
#   :api_url  - API Endpoint for the freegeoip server.
#   :num_retry - Number of times a 503 Service Unavailable, or
#                500 Internal Server Error attempt should be retried
#
# Output:
# {
#   ...
#   'geo_ip' => '188.32.194.21',
#   'geo_country_code' => 'RU',
#   'geo_country_name' => 'Russia',
#   'geo_region_code' => 'MOW',
#   'geo_region_name' => 'Moscow',
#   'geo_city' => 'Moscow',
#   'geo_zip_code' => '101976',
#   'geo_time_zone' => 'Europe/Moscow',
#   'geo_latitude' => 55.752,
#   'geo_longitude' => 37.616,
#   'geo_metro_code' => 0
# }

require 'rest_client'
require 'json'

module OptimusPrime
  module Transformers
    class GeoIP < Destination
      def initialize(ip_field:, api_url:, num_retry: 3)
        @ip_field = ip_field
        @api_url = api_url
        @num_retry = num_retry
        @retry_count = 0
        @finished = false
      end

      def write(record)
        push get_geoip record
      end

      private

      def get_geoip(record)
        @retry_count = 0
        @finished = false
        until @finished
          RestClient.get(@api_url + record[@ip_field]) do |response, request, result|
            record = handle_response response, record
          end
        end
        record
      end

      def handle_response(response, record)
        case response.code
        when 200
          handle_200 response.body, record
        when 404
          handle_404 response.code, record
        else
          handle_failed_response response.code, record
        end
      end

      def handle_200(body, record)
        JSON.parse(body).each do |key, value|
          record["geo_#{key}"] = value
        end
        @finished = true
        record
      end

      def handle_404(code, record)
        log_error code, record
        @finished = true
        record
      end

      def handle_failed_response(code, record)
        @retry_count += 1
        log_error code, record
        # Only retry on 503 and 504 responses
        if (code == 503 || code == 504) && @retry_count == @num_retry
          raise IOError.new("Geoip service unavailable - code: #{code} - record: #{record}")
        else
          raise IOError.new("Geoip service unhandled exception - code: #{code} - record: #{record}")
        end
      end

      def log_error(code, record)
        logger.error("Geoip lookup failed - code: #{code} - record: #{record}")
      end
    end
  end
end
