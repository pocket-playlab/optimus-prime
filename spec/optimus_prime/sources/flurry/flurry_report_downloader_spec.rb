require 'spec_helper'
require 'flurry_helpers'

describe OptimusPrime::Sources::FlurryHelpers::FlurryReportDownloader do
  let(:not_existing) { File.read 'spec/supports/flurry/not_existing.json' }
  let(:report_inexisting) { File.read 'spec/supports/flurry/report_inexisting.json' }
  let(:processing_report) { File.read 'spec/supports/flurry/processing_report.json' }

  let(:downloader) do
    report_uri = 'http://api.flurry.com/rawData/GetReport?apiAccessCode=SOMEFAKEAPIACCESSCODE&reportId=1114397'
    OptimusPrime::Sources::FlurryHelpers::FlurryReportDownloader.new(report_uri, 1, 1, Logger.new('/dev/null'))
  end

  context 'without report uri' do
    before { stub_flurry_request('GetReport', flurry_report(1_114_397), body: not_existing) }

    it 'should return nil directly' do
      expect(downloader.run).to eq nil
    end
  end

  context 'with report uri' do
    context 'inexisting report' do
      before { stub_flurry_request('GetReport', flurry_report(1_114_397), body: not_existing) }

      it 'should stop the loop and return nil' do
        dl = downloader
        expect(dl).to receive(:report_not_found).and_return(true)
        dl.run
      end
    end

    context 'unknown json message' do
      before do
        stub_flurry_request('GetReport', flurry_report(1_114_397),
                            body: '{"code":"200","message":"Something went wrong."}')
      end

      it 'should raise an exception' do
        error_message = 'Unknown Json Message: {"code"=>"200", "message"=>"Something went wrong."}'
        expect { downloader.run }.to raise_error error_message
      end
    end

    context 'report not ready' do
      before do
        stub_flurry_request('GetReport', flurry_report(1_114_397), { body: processing_report },
                            { body: processing_report }, body: events_file, content_type: 'application/octet-stream')
      end

      it 'should keep polling until the report is ready' do
        dl = downloader
        expect(dl).to receive(:sleep_and_log).twice.and_return(false)
        dl.run
      end
    end

    context 'report ready' do
      before do
        stub_flurry_request('GetReport', flurry_report(1_114_397),
                            body: events_file, content_type: 'application/octet-stream')
      end

      it 'should receive an octet-stream response' do
        dl = downloader
        expect(dl).to receive(:handle_octet_stream_response).and_return(true)
        dl.run
      end

      it 'should parse the report' do
        expect(downloader.run).to_not eq nil
      end
    end
  end
end
