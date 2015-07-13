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
    source.run_with.each do |event|
      expect(event.keys).to match_array [
        'Agency/PMD (af_prt)',
        'App Version',
        'Appsflyer Device Id',
        'Campaign (c)',
        'City',
        'Click Time',
        'Click URL',
        'Contributor 1',
        'Contributor 2',
        'Contributor 3',
        'Cost Per Install (af_cpi)',
        'Country Code',
        'Customer User Id',
        'Device Name',
        'Device Type',
        'IDFA',
        'IP',
        'Install Time',
        'Language',
        'MAC',
        'Media Source (pid)',
        'OS Version',
        'SDK Version',
        'Site Id (af_siteid)',
        'Sub Param 1 (af_sub1)',
        'Sub Param 2 (af_sub2)',
        'Sub Param 3 (af_sub3)',
        'Sub Param 4 (af_sub4)',
        'Sub Param 5 (af_sub5)',
        'WIFI'
      ]
    end
  end
end