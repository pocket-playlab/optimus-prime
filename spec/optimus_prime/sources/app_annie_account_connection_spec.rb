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

describe OptimusPrime::Sources::AppAnnieAccountConnectionList do
  describe '#each' do
    let(:pipeline) do
      OptimusPrime::Pipeline.new(
        {
          src: {
            class: 'OptimusPrime::Sources::AppAnnieAccountConnectionList',
            params: {
              api_key: 'api_key'
            }, next: ['dest']
          },
          dest: { class: 'OptimusPrime::Destinations::MyTestDestination' }
        },
        nil, {}, Logger.new('/dev/null')
      )
    end

    def stub_request_app_annie(body, url)
      url = "https://api.appannie.com/v1.2/accounts?#{url}"
      stub_request(:get, url).with(
        headers: {
          'Accept' => '*/*; q=0.5, application/xml',
          'Accept-Encoding' => 'gzip, deflate',
          'Authorization' => 'Bearer api_key',
          'User-Agent' => 'Ruby'
        }).to_return(status: 200, body: body, headers: {})
    end

    context 'one page response' do
      let(:response_body) { File.read('spec/supports/app_annie/account_connection_list_response.json') }
      it 'returns reponse body' do
        stub_request_app_annie(response_body, 'page_index=0')

        results = pipeline.start.wait.steps[:dest].written
        expect(results).to match_array [JSON.parse(response_body)]
      end
    end

    context 'multiple pages response' do
      let(:r1) { File.read('spec/supports/app_annie/acl_response_r1.json') }
      let(:r2) { File.read('spec/supports/app_annie/acl_response_r2.json') }
      it 'returns reponse body' do
        stub_request_app_annie(r1, 'page_index=0')
        stub_request_app_annie(r2, 'page_index=1')

        results = pipeline.start.wait.steps[:dest].written
        expect(results).to match_array [JSON.parse(r1), JSON.parse(r2)]
      end
    end
  end
end
