require 'spec_helper'
require 'json'

describe OptimusPrime::Sources::FlurryAppMetrics do
  let(:start_date) { '2015-08-01' }
  let(:end_date) { '2015-08-02' }
  let(:api_key) { SecureRandom.hex }
  let(:api_access_code) { SecureRandom.hex }

  let(:step) do
    OptimusPrime::Sources::FlurryAppMetrics.new(
      api_access_code: api_access_code, api_key: api_key, start_date: start_date, end_date: end_date,
      metric_name: metric_name, version: version, country: country, group_by: group_by)
  end

  def stub_flurry_request(body, status, optional_params = {})
    url = "http://api.flurry.com/appMetrics/#{metric_name}"
    stub_request(:get, url)
      .with(query: { apiAccessCode: api_access_code, apiKey: api_key, startDate: start_date,
                     endDate: end_date }.merge!(optional_params))
      .to_return(status: status, body: body)
  end

  context 'active users without any optional params' do
    let(:metric_name) { 'ActiveUsers' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_activeusers_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'active users with optional params' do
    let(:metric_name) { 'ActiveUsers' }
    let(:country) { 'TH' }
    let(:group_by) { 'DAYS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_activeusers_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'active users by week without any optional params' do
    let(:metric_name) { 'ActiveUsersByWeek' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_activeusersbyweek_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'active users by week with optional params' do
    let(:metric_name) { 'ActiveUsersByWeek' }
    let(:country) { 'TH' }
    let(:group_by) { 'WEEKS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_activeusersbyweek_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'active users by month without any optional params' do
    let(:metric_name) { 'ActiveUsersByMonth' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_activeusersbymonth_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'active users by month with optional params' do
    let(:metric_name) { 'ActiveUsersByMonth' }
    let(:country) { 'TH' }
    let(:group_by) { 'MONTHS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_activeusersbymonth_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'new users without any optional params' do
    let(:metric_name) { 'NewUsers' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_newusers_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'new users with optional params' do
    let(:metric_name) { 'NewUsers' }
    let(:country) { 'TH' }
    let(:group_by) { 'MONTHS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_newusers_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'median session length without any optional params' do
    let(:metric_name) { 'MedianSessionLength' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_mediansessionlength_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'median session length with optional params' do
    let(:metric_name) { 'MedianSessionLength' }
    let(:country) { 'TH' }
    let(:group_by) { 'MONTHS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_mediansessionlength_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'average session length without any optional params' do
    let(:metric_name) { 'AvgSessionLength' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_avgsessionlength_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'average session length with optional params' do
    let(:metric_name) { 'AvgSessionLength' }
    let(:country) { 'TH' }
    let(:group_by) { 'MONTHS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_avgsessionlength_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'sessions without any optional params' do
    let(:metric_name) { 'Sessions' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_sessions_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'sessions with optional params' do
    let(:metric_name) { 'Sessions' }
    let(:country) { 'TH' }
    let(:group_by) { 'MONTHS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_sessions_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'retained users without any optional params' do
    let(:metric_name) { 'RetainedUsers' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_retainedusers_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'retained users with optional params' do
    let(:metric_name) { 'RetainedUsers' }
    let(:country) { 'TH' }
    let(:group_by) { 'MONTHS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_retainedusers_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'page views without any optional params' do
    let(:metric_name) { 'PageViews' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_pageviews_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'page views with optional params' do
    let(:metric_name) { 'PageViews' }
    let(:country) { 'TH' }
    let(:group_by) { 'MONTHS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_pageviews_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'average page views per session without any optional params' do
    let(:metric_name) { 'AvgPageViewsPerSession' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { nil }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_avgpageviewspersession_nooptions.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'average page views per session with optional params' do
    let(:metric_name) { 'AvgPageViewsPerSession' }
    let(:country) { 'TH' }
    let(:group_by) { 'MONTHS' }
    let(:version) { '1.0' }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_avgpageviewspersession_options.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 200, country: country, groupBy: group_by, versionName: version)
      step.run_with.each do |result|
        expect(result).to eq JSON.parse(response_body)
      end
    end
  end

  context 'invalid version paramater' do
    let(:metric_name) { 'ActiveUsers' }
    let(:country) { nil }
    let(:group_by) { nil }
    let(:version) { 111 }

    let(:response_body) { File.read 'spec/supports/flurry/appmetrics_invalid_version.json' }

    it 'should return a valid json response' do
      stub_flurry_request(response_body, 107, versionName: version)
      expect { step.run_with.each }.to raise_error(Exception)
    end
  end

end
