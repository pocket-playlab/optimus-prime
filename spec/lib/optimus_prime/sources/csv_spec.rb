require 'spec_helper'

describe "CSV Source" do
  context "#initialize" do
    it 'should error when file not found' do
      expect { Csv.new(['col1','col2'], 'empty.csv') }.to raise_error
    end

    it 'should error when column missed' do
      expect { Csv.new(nil, 'empty.csv') }.to raise_error
    end
  end

  context "#get_data" do
    let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }

    context "when yaml file are correctly" do

      let(:install_attributes) { config.get_source_by_id('installs_report') }
      let(:csv_installs) { Csv.new(install_attributes['columns'], install_attributes['file_path']) }

      let(:incorrect_attributes) { config.get_source_by_id('install_incorrect_report') }
      let(:csv_incorrect) { Csv.new(incorrect_attributes['columns'], incorrect_attributes['file_path']) }

      it 'should found data in file' do
        expected_data = ['2015-01-17 14:20:00','2015-01-19 02:53:21',nil,'Facebook Ads',
          'Ranch Run - iOS - NOV15(TH)',nil,nil,'TH','Chon Buri','223.204.249.6','Yes','th-TH',
          '1421529619000-9924854',nil,'29083437-69C7-452D-B61A-CC138F709AF0',nil,'iPad','iPad mini 1G',
          '8.1','2.5.2.5.3.11','1.0.17.2',nil,nil,nil,nil,nil,nil,nil,nil,nil]
        expect(csv_installs.get_data[2]).to eq(expected_data)
      end

      it 'should return correct column number' do
        expect(csv_installs.get_data[0].count).to eq(install_attributes['columns'].count)
      end

      it 'should error when incorrect column number' do
        expect { csv_incorrect.get_data }.to raise_error('incorrect column number')
      end

    end

  end
end
