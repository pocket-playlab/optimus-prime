# The GeoIP Lookup transformer obtains the geographic information
# for the given IP address using the MaxMind GeoLite2 City database available
# at http://dev.maxmind.com/geoip/geoip2/geolite2/
#
# Geographic information from the MaxMind database will be added to the record
# inside of a geographic_info hash appended to the record
#
# Upon intialization, the module will check for a local copy of the MaxMind
# GeoLite2 City database. If there is no local copy it will be downloaded from the provided url.
#
# Parameters:
#   :ip_field - Field in the record containing the IP Address.
#   :maxmind_db_url - URL where the MaxMind database can be downloaded from.
#   :maxmind_db_file - Path to the MaxMind database file.
#
# Output:
# {
#   ...
#   'geographic_info' => {
#     'country_iso_code' => 'RU',
#     'country_name' => 'Russia',
#     'city' => 'Moscow',
#     'zip_code' => '101976',
#     'time_zone' => 'Europe/Moscow',
#     'continent_code' => 'EU',
#     'continent_name' => 'Europe',
#     'latitude' => 55.752,
#     'longitude' => 37.616,
#     'metro_code' => nil,
#     'subdivisions' => [
#       {
#         'iso_code => 'MOW',
#         'name' => 'Moscow'
#       }
#     ]
#   }
# }

require 'maxminddb'
require 'net/http'

module OptimusPrime
  module Transformers
    class GeoIP < Destination
      def initialize(ip_field:, maxmind_db_url:, maxmind_db_file:)
        @ip_field = ip_field
        @db_file = maxmind_db_file
        @gz_db_file = '/tmp/GeoLite2-City.mmdb.gz'
        @geo_field_name = 'geographic_info'

        download_database maxmind_db_url unless File.exist?(@db_file)

        @db = MaxMindDB.new(@db_file)
      end

      def write(record)
        push get_geoip record
      end

      private

      def get_geoip(record)
        result = @db.lookup(record[@ip_field])
        if result.found?
          record = add_fields result, record
        else
          logger.error("No GeoIP result found - #{record}")
        end
        record
      end

      def add_fields(result, record)
        record[@geo_field_name] = {}
        record = add_city result, record
        record = add_postal result, record
        record = add_location result, record
        record = add_continent result, record
        record = add_country result, record
        record = add_subdivisions result, record
        record
      end

      def add_city(result, record)
        record[@geo_field_name]['city'] = result.city.name
        record
      end

      def add_postal(result, record)
        record[@geo_field_name]['zip_code'] = result.postal.code
        record
      end

      def add_country(result, record)
        record[@geo_field_name]['country_iso_code'] = result.country.iso_code
        record[@geo_field_name]['country_name'] = result.country.name
        record
      end

      def add_continent(result, record)
        record[@geo_field_name]['continent_code'] = result.continent.code
        record[@geo_field_name]['continent_name'] = result.continent.name
        record
      end

      def add_location(result, record)
        record[@geo_field_name]['latitude'] = result.location.latitude
        record[@geo_field_name]['longitude'] = result.location.longitude
        record[@geo_field_name]['time_zone'] = result.location.time_zone
        record[@geo_field_name]['metro_code'] = result.location.metro_code
        record
      end

      def add_subdivisions(result, record)
        record[@geo_field_name]['subdivisions'] = result.subdivisions.map do |subdivision|
          { iso_code: subdivision.iso_code,
            name: subdivision.name }
        end
        record
      end

      def download_database(maxmind_db_url)
        uri = URI(maxmind_db_url)
        Net::HTTP.start(uri.host, uri.port) do |http|
          request = Net::HTTP::Get.new uri
          http.request request do |response|
            save_database_gz(response)
          end
        end
        uncompress_database_gz
      end

      def save_database_gz(response)
        open @gz_db_file, 'w' do |io|
          response.read_body do |chunk|
            print '.'
            io.write chunk
          end
        end
      end

      def uncompress_database_gz
        open @db_file, 'w' do |io|
          Zlib::GzipReader.open(@gz_db_file) do |gz|
            io.write gz.read
          end
        end
      end
    end
  end
end
