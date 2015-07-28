require 'spec_helper'
require 'ipaddr'

RSpec.describe OptimusPrime::Transformers::GeoIP do
  let(:ip_field) { 'ip_addr' }
  let(:maxmind_db_url) { 'http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz' }
  let(:maxmind_db_gz_file) { File.read('spec/supports/geoip/GeoLite2-City.mmdb.gz') }
  let(:maxmind_db_file) { '/tmp/GeoLite2-City.mmdb' }

  let(:logfile) { '/tmp/geoip_lookup.log' }
  let(:logger) { Logger.new(logfile) }

  let(:valid_input) do
    [
      {
        'name' => 'test',
        'ip_addr' => '188.32.194.21'
      }
    ]
  end

  let(:invalid_ip_address_input) do
    [
      {
        'name' => 'test',
        'ip_addr' => 'iamnotanipaddress'
      }
    ]
  end

  let(:success_output) do
    [
      {
        'name' => 'test',
        'ip_addr' => '188.32.194.21',
        'geographic_info' => {
          'country_iso_code' => 'RU',
          'country_name' => 'Russia',
          'continent_code' => 'EU',
          'continent_name' => 'Europe',
          'city' => 'Moscow',
          'zip_code' => '101976',
          'time_zone' => 'Europe/Moscow',
          'latitude' => 55.7522,
          'longitude' => 37.6156,
          'metro_code' => nil,
          'subdivisions' => [
            { 'iso_code' => 'MOW',
              'name' => 'Moscow' }
          ]
        }
      }
    ]
  end

  let(:step) do
    OptimusPrime::Transformers::GeoIP.new(ip_field: ip_field, maxmind_db_url: maxmind_db_url,
                                          maxmind_db_file: maxmind_db_file).log_to(logger)
  end

  before do
    stub_request(:get, maxmind_db_url).to_return(status: 200, body: maxmind_db_gz_file)
  end

  context 'download maxmind database' do
    it 'should download and decompress maxmind database to /tmp' do
      step.run_with(valid_input)
      expect(File.exist?(maxmind_db_file)).to be true
    end
  end

  context 'valid geoip lookup' do
    it 'adds geographic_info field with lookup values' do
      expect(step.run_with(valid_input)).to match_array success_output
    end
  end

  context 'invalid ip address geoip lookup' do
    it 'raises an exception' do
      expect { step.run_and_raise(invalid_ip_address_input) }.to raise_error IPAddr::InvalidAddressError
    end
  end
end
