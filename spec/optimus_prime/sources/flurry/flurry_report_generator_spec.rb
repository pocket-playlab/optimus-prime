require 'spec_helper'
require 'optimus_prime/sources/flurry_helpers/flurry_report_downloader'

describe OptimusPrime::Sources::FlurryHelpers::FlurryReportDownloader do
  let(:api_access_code) { SecureRandom.hex }
  let(:api_key) { SecureRandom.hex }

  let(:request_report_response) { File.read 'spec/supports/flurry/request_report_response.json' }
  let(:generating_report) { File.read 'spec/supports/flurry/generating_report.json' }

  let(:generator) do
    OptimusPrime::Sources::FlurryHelpers::FlurryReportGenerator.new api_access_code,
                                                                    api_key,
                                                                    Time.parse('2015-01-31'),
                                                                    Time.parse('2015-02-01'),
                                                                    1,
                                                                    Logger.new(STDERR)
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

  context 'HTTP' do
    context 'Too Many Requests' do
      before do
        stub_flurry_request 'http://api.flurry.com/rawData/Events', flurry_params,
                            { body: '', status: 429 },
                            { body: '', status: 429 },
                            body: request_report_response
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
            stub_flurry_request 'http://api.flurry.com/rawData/Events', flurry_params,
                                { body: generating_report, status: 500 },
                                body: request_report_response
          end

          it 'should sleep and retry' do
            gen = generator
            expect(gen).to receive(:sleep_and_log).with(1).and_return(false)
            gen.run
          end
        end

        context 'no report being generated' do
          before do
            stub_flurry_request 'http://api.flurry.com/rawData/Events', flurry_params,
                                body: request_report_response
          end

          it 'should get a report uri back' do
            expect(generator.run).to eq 'http://api.flurry.com/rawData/GetReport?apiAccessCode=SOMEFAKEAPIACCESSCODE&reportId=1114396'
          end
        end

        context 'unknown message' do
          before do
            stub_flurry_request 'http://api.flurry.com/rawData/Events', flurry_params,
                                body: '{"code":"200","message":"Something"}'
          end

          it 'should raises an exception' do
            error_message = 'Unknown Json Message: {"code"=>"200", "message"=>"Something"}'
            expect { generator.run }.to raise_error error_message
          end
        end
      end
    end

    context 'Unknown Code' do
      before do
        stub_flurry_request 'http://api.flurry.com/rawData/Events', flurry_params,
                            body: '', status: 0
      end

      it 'should raise an exception' do
        expect { generator.run }.to raise_error 'Unhandled HTTP Status: Net::HTTPUnknownResponse.'
      end
    end
  end
end
