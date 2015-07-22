require 'rest_client'
require 'json'

module OptimusPrime
  module Transformers
    class GeoIP < Destination

      def initialize(ip_field:)
        @ip_field = ip_field
        @url = "https://freegeoip.net/json/"
      end

      def write(record)
        push lookup_geoip record
      end

      private

      def lookup_geoip(record)
        # Make REST call for ip provied in @ip_field
        resp = JSON.parse(RestClient.get(@url + record[@ip_field]).body)
        record['geo_country_code'] = resp["country_code"]
        record['geo_country_name'] = resp["country_name"]
        record['geo_region_code'] = resp["region_code"]
        record['geo_region_name'] = resp["region_name"]
        record['geo_city'] = resp["city"]
        record['geo_zip_code'] = resp["zip_code"]
        record['geo_time_zone'] = resp["time_zone"]
        record['geo_latitude'] = resp["latitude"]
        record['geo_longitude'] = resp["longitude"]
        record['geo_metro_code'] = resp["metro_code"]
        record
      end

    end
  end
end
