require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::GeoIP do
  let(:ip_field) { 'ip_addr' }
  let(:api_url) { 'https://freegeoip-prod.pocketplaylab.com/json/' }

  let(:response_1) { File.read 'spec/supports/geoip/response_1.json' }

  let(:input) do
    [
      {
        'name' => 'test',
        'ip_addr' => '188.32.194.21'
      }
    ]
  end

  let(:output) do
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

  before do
    stub_request(:get, api_url + "188.32.194.21")
      .to_return(status: 200, body: response_1)
  end

  context 'lookup geoip information' do
    it 'adds geo_* fields with lookup values' do
      step = OptimusPrime::Transformers::GeoIP.new(ip_field: ip_field, api_url: api_url)
      expect(step.run_with(input)).to match_array output
    end
  end
end
