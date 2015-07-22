# The GeoIP Lookup transformer obtains the geographic information
# for the given IP address.
#
# Geographic information from the freegeoip API will be added to the record
# by appending geo_ for each key from the JSON response.
#
# Parameters:
#   :ip_field - Field in the record containing the IP Address.
#   :api_url  - API Endpoint for the freegeoip server.
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

      def initialize(ip_field:, api_url:)
        @ip_field = ip_field
        @api_url = api_url
      end

      def write(record)
        push lookup_geoip record
      end

      private

      def lookup_geoip(record)
        resp = JSON.parse(RestClient.get(@api_url + record[@ip_field]).body)
        resp.each do |key, value|
          record["geo_#{key}"] = value
        end
        record
      end
    end
  end
end
