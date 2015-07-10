require 'spec_helper'

module OptimusPrime
  module Destinations
    class MyTestDestination < Destination
      attr_reader :written
      def initialize
        @written = []
      end

      def write(record)
        @written << record
      end
    end
  end
end

describe OptimusPrime::Sources::AppAnnieProductSales do
  describe '#each' do
    let(:pipeline) do
      OptimusPrime::Pipeline.new(
        {
          src: {
            class: 'OptimusPrime::Sources::AppAnnieProductSales',
            params: {
              api_key: 'api_key',
              account_id: 'acc_id',
              product_id: 'prod_id',
              start_date: '2015-01-01',
              end_date: '2015-01-01',
              options: { break_down: 'date+country+iap' }
            }, next: ['dest']
          },
          dest: { class: 'OptimusPrime::Destinations::MyTestDestination' }
        },
        nil, {}, Logger.new('/dev/null')
      )
    end

    let(:response_body) { File.read('spec/supports/app_annie/one_page_response.json') }
    let(:response_body_1) { File.read('spec/supports/app_annie/response_1.json') }
    let(:response_body_2) { File.read('spec/supports/app_annie/response_2.json') }

    def stub_request_app_annie(body, params)
      url = "https://api.appannie.com/v1.2/accounts/acc_id/products/prod_id/sales?#{params}"
      stub_request(:get, url).with(
        headers: {
          'Accept' => '*/*; q=0.5, application/xml',
          'Accept-Encoding' => 'gzip, deflate',
          'Authorization' => 'Bearer api_key',
          'User-Agent' => 'Ruby'
        }).to_return(status: 200, body: body, headers: {})
    end

    context 'one page response' do
      it 'returns reponse body' do
        stub_request_app_annie(
          response_body,
          'break_down=date%2Bcountry%2Biap&end_date=2015-01-01&start_date=2015-01-01'
        )

        results = pipeline.start.wait.steps[:dest].written
        expect(results).to match_array [JSON.parse(response_body)]
      end
    end

    context 'multiple pages response' do
      it 'returns response body' do
        stub_request_app_annie(
          response_body_1,
          'break_down=date%2Bcountry%2Biap&end_date=2015-01-01&start_date=2015-01-01'
        )
        stub_request_app_annie(
          response_body_2,
          'break_down=date%20country%20iap&end_date=2015-01-01&page_index=1&start_date=2015-01-01'
        )

        results = pipeline.start.wait.steps[:dest].written
        expect(results).to match_array [JSON.parse(response_body_1), JSON.parse(response_body_2)]
      end
    end
  end
end
