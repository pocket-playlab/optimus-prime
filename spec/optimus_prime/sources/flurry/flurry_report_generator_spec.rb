require 'spec_helper'
require 'flurry_helpers'
require 'optimus_prime/sources/flurry_helpers/flurry_report_downloader'

describe OptimusPrime::Sources::FlurryHelpers::FlurryReportDownloader do
  let(:generating_report) { File.read 'spec/supports/flurry/generating_report.json' }

  let(:generator) do
    OptimusPrime::Sources::FlurryHelpers::FlurryReportGenerator.new(
      api_access_code, api_key, Time.parse('2015-01-31'), Time.parse('2015-02-01'), 1, Logger.new('/dev/null')
    )
  end

  context 'HTTP' do
    context 'Too Many Requests' do
      before do
        stub_flurry_request('Events', flurry_params,
          { body: '', status: 429 }, { body: '', status: 429 }, body: request_report_response)
      end

      it 'should sleep and retry' do
        gen = generator
        expect(gen).to receive(:sleep_and_log).twice.with(1).and_return(false)
        gen.run
      end
    end

    context 'OK' do
      context 'json' do
        context 'report being generated' do
          before do
            stub_flurry_request 'Events', flurry_params,
              { body: generating_report, status: 500 }, body: request_report_response
          end

          it 'should sleep and retry' do
            gen = generator
            expect(gen).to receive(:sleep_and_log).with(1).and_return(false)
            gen.run
          end
        end

        context 'no report being generated' do
          before { stub_flurry_request('Events', flurry_params, body: request_report_response) }

          it 'should get a report uri back' do
            expect(generator.run).to eq 'http://api.flurry.com/rawData/GetReport?apiAccessCode=SOMEFAKEAPIACCESSCODE&reportId=1114396'
          end
        end

        context 'unknown message' do
          before { stub_flurry_request('Events', flurry_params, body: '{"code":"200","message":"Something"}') }

          it 'should raises an exception' do
            error_message = 'Unknown Json Message: {"code"=>"200", "message"=>"Something"}'
            expect { generator.run }.to raise_error error_message
          end
        end
      end
    end

    context 'Unknown Code' do
      before { stub_flurry_request('Events', flurry_params, body: '', status: 0) }

      it 'should raise an exception' do
        expect { generator.run }.to raise_error 'Unhandled HTTP Status: Net::HTTPUnknownResponse.'
      end
    end
  end
end
