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
#
# If there was a non 503 or 500 error then the record will be returned with
# the geo_ fields ommitted and the error will be logged

require 'rest_client'
require 'json'

module OptimusPrime
  module Transformers
    class GeoIP < Destination

      def initialize(ip_field:, api_url:, num_retry: 3)
        @ip_field = ip_field
        @api_url = api_url
        @num_retry = num_retry
      end

      def write(record)
        push get_geoip record
      end

      private

      def get_geoip(record)
        retry_count = 0

        while retry_count < @num_retry
          RestClient.get(@api_url + record[@ip_field]) do |response, request, result|
            case response.code
            when 200
              JSON.parse(response.body).each do |key, value|
                record["geo_#{key}"] = value
              end
              return record
            when 503, 500
              retry_count += 1
              if retry_count == @num_retry
                raise IOError, "Geoip service unavailable - code: #{response.code} - record: #{record}"
              end
            else
              logger.error("Geoip lookup failed - code: #{response.code} - record: #{record}")
              return record
            end
          end
        end
        record
      end
    end
  end
end
