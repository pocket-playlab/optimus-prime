require 'spec_helper'

describe OptimusPrime::Sources::Appsflyer do
  let(:app_id) { SecureRandom.hex }
  let(:api_token) { SecureRandom.hex }
  let(:installs_report) { File.read 'spec/supports/appsflyer/installs.csv' }
  let(:source) do
    OptimusPrime::Sources::Appsflyer.new(
      app_id: app_id, api_token: api_token, report_type: 'installs',
      from: Time.utc(2015, 1, 12), to: Time.utc(2015, 1, 19)
    )
  end

  before do
    stub_request(:get, "https://hq.appsflyer.com/export/#{app_id}/installs_report")
      .with(query: { api_token: api_token, from: '2015-01-12', to: '2015-01-19' })
      .to_return(status: 200, body: installs_report)
  end

  it 'should yield events' do
    expect(source.run_with.first).to eq(installs_report.force_encoding('ASCII-8BIT'))
  end
end