require 'spec_helper'
require 'sequel'

describe "SQLite Source" do

  let(:columns) { { 'col1': 'String', 'col2': 'String' } }

  context "#initialize" do

    context "when missing parameter" do
      it { expect { Sqlite.new }.to raise_error }
      it { expect { Sqlite.new(columns, 'path_to_db') }.to raise_error }
      it { expect { Sqlite.new(columns, 'path_to_db', nil) }.to raise_error('columns, db_path and query are required') }
      it { expect { Sqlite.new(nil, 'path_to_db', '12321') }.to raise_error('columns, db_path and query are required') }
      it { expect { Sqlite.new(columns, nil, '12321') }.to raise_error('columns, db_path and query are required') }
    end

    context "when parameters correctly" do
      it 'should created instance' do 
        sqlite = Sqlite.new(columns, 'database.db', 'select * from table')
        expect(sqlite.columns).to eq({ 'col1': 'String', 'col2': 'String'})
      end
    end

    context 'instantiate with sources.yml file' do

      let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }
      let(:sqlite_attributes) { config.get_source_by_id('sqlite3_game_level_database') }

      it 'should created instance' do
        sqlite_object = Sqlite.new(sqlite_attributes['columns'], sqlite_attributes['file_path'], sqlite_attributes['query'])
        expect(sqlite_object.columns).to eq(sqlite_attributes['columns'])
        expect(sqlite_object.query).to eq(sqlite_attributes['query'])
      end

    end
    
  end

  context "#retrieve_data" do

    let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }

    context "configuration correct" do
      let(:sqlite_attributes) { config.get_source_by_id('sqlite3_game_level_database') }
      let(:sqlite_object) { Sqlite.new(sqlite_attributes['columns'], sqlite_attributes['file_path'], sqlite_attributes['query']) }

      it 'should return array data' do
        expected_data = [1, 'dragon_cube', 50]
        expect(sqlite_object.retrieve_data.first).to eq(expected_data)
      end
    end

    context "query incorrect" do
      let(:sqlite_attributes) { config.get_source_by_id('game_level_db_incorrect') }
      let(:sqlite_object) { Sqlite.new(sqlite_attributes['columns'], sqlite_attributes['file_path'], sqlite_attributes['query']) }

      it 'should error' do
        expect { sqlite_object.retrieve_data }.to raise_error
      end
    end
  end
end
