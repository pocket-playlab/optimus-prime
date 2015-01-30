require 'spec_helper'
require 'sequel'

describe "SQLite Source" do
  context "#initialize" do
    it 'should error when column missed' do
      expect { Sqlite.new }.to raise_error
    end
  end

  context "#retrieve_data" do
    let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }

    context "when yaml file are correctly" do

      let(:sqlite_attributes) { config.get_source_by_id('sqlite_sample') }
      let(:sqlite_object) { Sqlite.new(sqlite_attributes['columns']) }

      it 'should found data in file' do
        expected_data = [1, 'item1', 10000]
        expect(sqlite_object.retrieve_data[0]).to eq(expected_data)
      end

      it 'should return correct column number' do
        expect(sqlite_object.retrieve_data[0].count).to eq(sqlite_attributes['columns'].count)
      end

    end

  end
end
