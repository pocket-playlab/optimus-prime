require 'spec_helper'

describe Appsflyer do

  let(:appsflyer_token) { SecureRandom.hex }

  let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }

  let(:appsflyer_attributes) { config.get_source_by_id('appsflyer') }

  let(:installs_report) { File.read(File.expand_path '../../../../supports/installs.csv', __FILE__) }
  let(:installs_second_report) { File.read(File.expand_path '../../../../supports/installs_second.csv', __FILE__) }

  before do
    ENV['APPSFLYER_API_TOKEN'] = appsflyer_token
  end

  context "#initialize" do

    context "when response status is 200" do

      before do
          stub_request(:get, "https://hq.appsflyer.com/export/id855124397/installs_report?api_token=#{appsflyer_token}&from=#{Date.today}&to=#{Date.today}")
          .with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'User-Agent'=>'Ruby'})
          .to_return(:status => 200, :body => installs_report, :headers => {})

          stub_request(:get, "https://hq.appsflyer.com/export/id855124397/installs_report?api_token=#{appsflyer_token}&from=#{Date.today - 2}&to=#{Date.today - 1}")
          .with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'User-Agent'=>'Ruby'})
          .to_return(:status => 200, :body => installs_second_report, :headers => {})
      end

      context "when missing parameter" do
        it { expect { appsflyer = Appsflyer.new }.to raise_error }
        it { expect { appsflyer = Appsflyer.new(['col1', 'col2'], 'installs_report') }.to raise_error }
        it { expect { appsflyer = Appsflyer.new(['col1', 'col2'], nil, '123421') }.to raise_error('columns, report_type and app_id are required') }
        it { expect { appsflyer = Appsflyer.new(nil, 'installs_report', '12321') }.to raise_error('columns, report_type and app_id are required') }
      end

      context "default date" do
        it 'should create instance when all parameter are collect' do
          appsflyer = Appsflyer.new(['col1', 'col2'], 'installs_report', 'id855124397')
          expect(appsflyer.columns).to eq(['col1', 'col2'])
          expect(appsflyer.data).to eq(installs_report)
        end
      end

      context "input date" do
        it 'should create instance with date' do
          appsflyer = Appsflyer.new(['col1', 'col2'], 'installs_report', 'id855124397', from_date: Date.today - 2, to_date: Date.today - 1 )
          expect(appsflyer.columns).to eq(['col1', 'col2'])
          expect(appsflyer.data).to eq(installs_second_report)
        end
      end
    end

    context 'sources.yml file' do
      it 'should create instance' do
        appsflyer = Appsflyer.new(appsflyer_attributes['columns'], appsflyer_attributes['report_type'], appsflyer_attributes['app_id'])
        expect(appsflyer.columns).to eq(appsflyer_attributes['columns'])
      end    
    end

    
  end

  context "#retrieve_data" do

    let(:appsflyer_instance) { Appsflyer.new(appsflyer_attributes['columns'], appsflyer_attributes['report_type'], appsflyer_attributes['app_id']) }

    context "when response status is 200" do
      before do 
          stub_request(:get, "https://hq.appsflyer.com/export/id855124397/installs_report?api_token=#{appsflyer_token}&from=#{Date.today}&to=#{Date.today}")
          .with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'User-Agent'=>'Ruby'})
          .to_return(:status => 200, :body => installs_report, :headers => {})
      end

      it 'should return array as an expected_array' do
        expected_array = CSV.parse(installs_report)
        expect(appsflyer_instance.retrieve_data).to eq(expected_array)
      end
    end

    context "when response status is 302" do
      before do 
          stub_request(:get, "https://hq.appsflyer.com/export/id855124397/installs_report?api_token=#{appsflyer_token}&from=#{Date.today}&to=#{Date.today}")
          .with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'User-Agent'=>'Ruby'})
          .to_return(:status => 302, :body => "", :headers => { 'location' => 'https://second-link.com' })

          stub_request(:get, "https://second-link.com")
          .with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'User-Agent'=>'Ruby'})
          .to_return(:status => 200, :body => installs_second_report, :headers => {})
      end
      it 'should receive report on second url' do
        expected_array = CSV.parse(installs_second_report)
        expect(appsflyer_instance.retrieve_data).to eq(expected_array)
      end
    end

    context "when response status is 400" do
      before do 
          stub_request(:get, "https://hq.appsflyer.com/export/id855124397/installs_report?api_token=#{appsflyer_token}&from=#{Date.today}&to=#{Date.today}")
          .with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'User-Agent'=>'Ruby'})
          .to_return(:status => 400, :body => "", :headers => {})
      end

      it 'should error' do
        expect { appsflyer_instance.retrieve_data }.to raise_error
      end
    end

    context "when response status is 404" do
      before do 
          stub_request(:get, "https://hq.appsflyer.com/export/id855124397/installs_report?api_token=#{appsflyer_token}&from=#{Date.today}&to=#{Date.today}")
          .with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'User-Agent'=>'Ruby'})
          .to_return(:status => 404, :body => "", :headers => {})
      end

      it 'should show error on console' do
        expect { appsflyer_instance.retrieve_data }.to raise_error
      end
    end

    context "when response status is 500" do
      before do
          stub_request(:get, "https://hq.appsflyer.com/export/id855124397/installs_report?api_token=#{appsflyer_token}&from=#{Date.today}&to=#{Date.today}")
          .with(:headers => {'Accept'=>'*/*', 'Accept-Encoding'=>'gzip;q=1.0,deflate;q=0.6,identity;q=0.3', 'User-Agent'=>'Ruby'})
          .to_return(:status => 500, :body => "", :headers => {})
      end

      it 'should show error on console' do
        expect { appsflyer_instance.retrieve_data }.to raise_error
      end
    end

  end

end