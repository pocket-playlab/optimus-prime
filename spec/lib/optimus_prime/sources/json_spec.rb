# json reference file : http://www.sitepoint.com/iphone-menu-json-example/

require 'spec_helper'

describe "Json Source" do
  context "#initialize" do
    it 'should error when file not found' do
      expect { Json.new(['col1','col2'], 'empty.json') }.to raise_error
    end

    it 'should error when column missed' do
      expect { Json.new(nil, 'empty.json') }.to raise_error
    end
  end

  context "#retrieve_data" do
    let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }

    context "when yaml file are correctly" do

      let(:json_attributes) { config.get_source_by_id('json_sample') }
      let(:json_object) { Json.new(json_attributes['columns'], json_attributes['file_path']) }

      it 'should found data in file' do
        expected_data = ['Open', nil]
        expect(json_object.retrieve_data[0]).to eq(expected_data)
      end

      it 'should return correct column number' do
        expect(json_object.retrieve_data[0].count).to eq(json_attributes['columns'].count)
      end

    end

  end
end
