require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::GeoIP do
  let(:ip_field) { 'ip_addr' }
  let(:api_url) { 'https://freegeoip.net/json/' }
  let(:num_retry) { 3 }

  let(:stub_url) { api_url + '188.32.194.21' }
  let(:logfile) { '/tmp/geoip_lookup.log' }
  let(:logger) { Logger.new(logfile) }

  let(:response_success) { File.read('spec/supports/geoip/response_1.json') }
  let(:response_404) { File.read('spec/supports/geoip/404_response.txt') }

  let(:input) do
    [
      {
          'name' => 'test',
          'ip_addr' => '188.32.194.21'
      }
    ]
  end

  let(:success_output) do
    [
      {
        'name' => 'test',
        'ip_addr' => '188.32.194.21',
        'geo_ip' => '188.32.194.21',
        'geo_country_code' => 'RU',
        'geo_country_name' => 'Russia',
        'geo_region_code' => 'MOW',
        'geo_region_name' => 'Moscow',
        'geo_city' => 'Moscow',
        'geo_zip_code' => '101976',
        'geo_time_zone' => 'Europe/Moscow',
        'geo_latitude' => 55.752,
        'geo_longitude' => 37.616,
        'geo_metro_code' => 0
      }
    ]
  end

  let(:failed_output) do
    [
      {
        'name' => 'test',
        'ip_addr' => '188.32.194.21',
      }
    ]
  end

  let(:step) do
      OptimusPrime::Transformers::GeoIP.new(ip_field: ip_field, api_url: api_url, num_retry: 3).log_to(logger)
  end

  context 'valid geoip lookup' do
    before do
      stub_request(:get, stub_url)
        .to_return(status: 200, body: response_success)
    end

    it 'adds geo_* fields with lookup values' do
      expect(step.run_with(input)).to match_array success_output
    end
  end

  context 'geoip lookup 404' do
    before do
      stub_request(:get, stub_url)
        .to_return(status: 404, body: response_404)
    end

    it 'does not add geo_ fields' do
      expect(step.run_with(input)).to match_array failed_output
    end
  end

  context 'geoip lookup unavailable' do
    before do
      stub_request(:get, stub_url)
        .to_return(status: 503)
    end

    it 'raises an exception' do
      expect { step.run_with(input) }.to raise_error(IOError)
    end

  end
end
