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

describe OptimusPrime::Sources::AppAnnieCampaignList do
  describe '#each' do
    let(:pipeline) do
      OptimusPrime::Pipeline.new(
        {
          src: {
            class: 'OptimusPrime::Sources::AppAnnieCampaignList',
            params: {
              api_key: 'api_key',
              market: 'ios',
              product_id: 'prod_id'
            }, next: ['dest']
          },
          dest: { class: 'OptimusPrime::Destinations::MyTestDestination' }
        },
        nil, {}, Logger.new('/dev/null')
      )
    end

    let(:response_body) { File.read('spec/supports/app_annie/campaign_list_response.json') }
    let(:response_body_p1) { File.read('spec/supports/app_annie/campaign_list_response_p1.json') }
    let(:response_body_p2) { File.read('spec/supports/app_annie/campaign_list_response_p2.json') }

    def stub_request_app_annie(body, params)
      url = "https://api.appannie.com/v1.2/apps/ios/app/prod_id/ad_items#{params}"
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
        stub_request_app_annie(response_body,'')

        results = pipeline.start.wait.steps[:dest].written
        expect(results).to match_array [JSON.parse(response_body)]
      end
    end

    context 'multiple pages response' do
      it 'returns response body' do
        stub_request_app_annie(
          response_body_p1,
          ''
        )
        stub_request_app_annie(
          response_body_p2,
          '?page_index=1'
        )
        results = pipeline.start.wait.steps[:dest].written
        expect(results).to match_array [JSON.parse(response_body_p1), JSON.parse(response_body_p2)]
      end
    end
  end
end
