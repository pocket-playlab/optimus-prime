require 'spec_helper'

describe "CSV Source" do
  context "installs report" do
    let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }

    it 'should return csv report when yaml file are correctly' do
      attributes = config.get_source_by_id('installs_report')
      file_path = attributes['file_path']
      csv = Csv.new(attributes['columns'], file_path)
      expected_data = ['2015-01-17 14:20:00','2015-01-19 02:53:21',nil,'Facebook Ads',
        'Ranch Run - iOS - NOV15(TH)',nil,nil,'TH','Chon Buri','223.204.249.6','Yes','th-TH',
        '1421529619000-9924854',nil,'29083437-69C7-452D-B61A-CC138F709AF0',nil,'iPad','iPad mini 1G',
        '8.1','2.5.2.5.3.11','1.0.17.2',nil,nil,nil,nil,nil,nil,nil,nil,nil]
      expect(csv.retrieve_data[2]).to eq(expected_data)
    end
  end

  before do
    
  end

  
end

# expected_data = []
# 
# csv.get_data deeply_equals expected_data