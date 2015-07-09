require 'spec_helper'
require 'flurry_helpers'
require 'optimus_prime/sources/flurry'

describe OptimusPrime::Sources::Flurry do
  let(:not_existing) { File.read 'spec/supports/flurry/not_existing.json' }
  let(:report_uri) { nil }
  let(:step) do
    OptimusPrime::Sources::Flurry.new(
      api_access_code: api_access_code, api_key: api_key, start_time: '2015-01-31', end_time: '2015-02-01',
      poll_interval: 0.5, report_uri: report_uri, retry_interval: 1)
    .suppress_log
  end

  before :each do
    stub_flurry_request('Events', flurry_params, body: request_report_response)
    stub_flurry_request('GetReport', flurry_report(1_114_396), body: events_file, content_type: 'application/octet-stream')
  end

  context 'without report uri' do
    it 'should yield events' do
      records = step.run_with.to_a
      expect(records.count).to eq 3
      expect(records[0].keys).to include('Session', 'Version', 'Device', 'Event', 'Timestamp')
    end
  end

  context 'with report uri' do
    let(:report_uri) { 'http://api.flurry.com/rawData/GetReport?apiAccessCode=SOMEFAKEAPIACCESSCODE&reportId=1114397' }
    before do
      stub_flurry_request('GetReport', flurry_report(1_114_397), body: events_file, content_type: 'application/octet-stream')
    end

    it 'should use report_uri if present' do
      expect(step.run_with.to_a.count).to eq 3
    end
  end

  context 'with report uri not existing' do
    before { stub_flurry_request('GetReport', flurry_report(1_114_397), body: not_existing) }

    it 'should fall back to regular request' do
      expect(step.run_with.to_a.count).to eq 3
    end
  end

  context 'api over limit' do
    before { stub_flurry_request('Events', flurry_params, { body: '', status: 429 }, body: request_report_response) }

    it 'should retry 2s later' do
      expect(step.run_with.to_a.count).to eq 3
    end
  end

  context 'report already being generated' do
    before do
      generating_report = File.read('spec/supports/flurry/generating_report.json')
      stub_flurry_request('Events', flurry_params, { body: generating_report, status: 500 }, body: request_report_response)
    end

    it 'should fail, wait, retry and succeed' do
      expect(step.run_with.to_a.count).to eq 3
    end
  end
end
