require 'spec_helper'

RSpec.describe OptimusPrime::Utilities::CurrencyLayer do
  let(:currency_layer_response) { File.read('spec/supports/utilities/currency_layer_response.json') }
  let(:currency_layer_expected) { File.read('spec/supports/utilities/currency_layer_expected.json') }
  let(:currency_layer_error) { File.read('spec/supports/utilities/currency_layer_error.json') }

  let(:currency_file) { '/tmp/currency.json' }
  let(:expected_currencies) { '/' }
  let(:curr_layer) { OptimusPrime::Utilities::CurrencyLayer }

  def stub_currency_layer(api_key, body)
    stub_request(:get, "http://apilayer.net/api/live?access_key=#{api_key}&source=USD")
      .with(headers: {
              'Accept' => '*/*; q=0.5, application/xml',
              'Accept-Encoding' => 'gzip, deflate',
              'User-Agent' => 'Ruby'
            })
      .to_return(status: 200, body: body, headers: {})
  end

  def delete_file_before(file_name)
    File.delete(file_name) if File.exist? file_name
  end

  context 'connect with the right api keys' do
    before do
      stub_currency_layer('access_key_test', currency_layer_response)
      delete_file_before('/tmp/currency.json')
      @currency_layer = curr_layer.new(
        access_key: 'access_key_test',
        options: { source: 'USD' })
    end

    it 'create currency file in /tmp ' do
      expect(File.exist?(currency_file)).to be true
    end

    it 'removes usd prefix in quotes key' do
      expect(@currency_layer.currencies_rate).to eq JSON.parse(currency_layer_expected)
    end
  end

  context 'connect with wrong api keys' do
    before do
      stub_currency_layer('access_key_error', currency_layer_error)
      delete_file_before('/tmp/currency.json')
    end

    it 'raise error when it got invalid access key' do
      expect { curr_layer.new(access_key: 'access_key_error', options: { source: 'USD' }) }.to raise_error
    end
  end
end
