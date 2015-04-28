require 'spec_helper'
require 'optimus_prime/sources/flurry'

describe OptimusPrime::Sources::Flurry do
  let(:api_access_code) { SecureRandom.hex }
  let(:api_key) { SecureRandom.hex }

  let(:request_report_response) { File.read 'spec/supports/flurry/request_report_response.json' }
  let(:not_existing) { File.read 'spec/supports/flurry/not_existing.json' }
  let(:events) { File.read 'spec/supports/flurry/events.json.gz' }
  let(:generating_report) { File.read 'spec/supports/flurry/generating_report.json' }
  let(:report_inexisting) { File.read 'spec/supports/flurry/report_inexisting.json' }

  let(:source) do
    src = OptimusPrime::Sources::Flurry.new api_access_code: api_access_code,
                                            api_key: api_key,
                                            start_time: '2015-01-31',
                                            end_time: '2015-02-01',
                                            poll_interval: 0.5,
                                            report_uri: nil,
                                            retry_interval: 1
    src.logger = Logger.new(STDERR)
    src
  end

  let(:source_with_report_uri) do
    report_uri = 'http://api.flurry.com/rawData/GetReport?apiAccessCode=SOMEFAKEAPIACCESSCODE&reportId=1114397'
    OptimusPrime::Sources::Flurry.new(api_access_code: api_access_code,
                                      api_key: api_key,
                                      start_time: '2015-01-31',
                                      end_time: '2015-02-01',
                                      poll_interval: 0.5,
                                      report_uri: report_uri).tap do |src|
      src.logger = Logger.new(STDERR)
    end
  end

  def flurry_params
    { apiAccessCode: api_access_code, apiKey: api_key,
      startTime: '1422662400000', endTime: '1422748800000' }
  end

  def flurry_report(report_id)
    { apiAccessCode: 'SOMEFAKEAPIACCESSCODE', reportId: report_id }
  end

  def stub_flurry_request(url, params, *responses)
    responses = responses.map do |response|
      { status: response[:status] || 200, body: response[:body],
        headers: { 'content-type' => response[:content_type] || 'application/json' } }
    end

    stub_request(:get, url).with(query: params).to_return(responses)
  end

  before do
    stub_flurry_request 'http://api.flurry.com/rawData/Events', flurry_params,
                        body: request_report_response

    stub_flurry_request 'http://api.flurry.com/rawData/GetReport', flurry_report(1_114_396),
                        body: events, content_type: 'application/octet-stream'
  end

  context 'without report uri' do
    it 'should yield events' do
      events = source.to_a

      expect(events.count).to eq 3
      expect(events[0].keys).to include('Session', 'Version', 'Device', 'Event', 'Timestamp')
    end
  end

  context 'with report uri' do
    before do
      stub_flurry_request 'http://api.flurry.com/rawData/GetReport', flurry_report(1_114_397),
                          body: events, content_type: 'application/octet-stream'
    end

    it 'should use report_uri if present' do
      events = source_with_report_uri.to_a

      expect(events.count).to eq 3
    end
  end

  context 'with report uri not existing' do
    before do
      stub_flurry_request 'http://api.flurry.com/rawData/GetReport', flurry_report(1_114_397),
                          body: not_existing
    end

    it 'should fall back to regular request' do
      events = source_with_report_uri.to_a
      expect(events.count).to eq 3
    end
  end

  context 'api over limit' do
    before do
      stub_flurry_request 'http://api.flurry.com/rawData/Events', flurry_params,
                          { body: '', status: 429 },
                          body: request_report_response
    end

    it 'should retry 2s later' do
      events = source.to_a
      expect(events.count).to eq 3
    end
  end

  context 'report already being generated' do
    before do
      stub_flurry_request 'http://api.flurry.com/rawData/Events', flurry_params,
                          { body: generating_report, status: 500 },
                          body: request_report_response
    end

    it 'should fail, wait, retry and succeed' do
      events = source.to_a
      expect(events.count).to eq 3
    end
  end
end
