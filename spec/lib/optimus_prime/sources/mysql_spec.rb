require 'spec_helper'
require 'sequel'

describe MySQL do
  context "#initialize" do

    context "when missing parameter" do
      it { expect { MySQL.new }.to raise_error }
      it { expect { MySQL.new(['col1', 'col2'], 'host', 'username', 'password') }.to raise_error }
      it { expect { MySQL.new(['col1', 'col2'], nil, 'username', 'password', 'db_selected', 'select *') }.to raise_error('cannot connect database') }
      it { expect { MySQL.new(['col1', 'col2'], 'host', 'username', 'password', nil, 'select *') }.to raise_error('cannot connect database') }
      it { expect { MySQL.new(['col1', 'col2'], 'host', 'username', nil, 'db_name', 'select *') }.to raise_error('cannot connect database') }
      it { expect { MySQL.new(nil, 'host', 'username', 'password', 'db_name', 'select *') }.to raise_error('columns required') }
      it { expect { MySQL.new(['col1', 'col2'], 'host', 'username', 'password', 'db_name', nil) }.to raise_error('query required') }
    end

    context "when parameters correctly" do
      before do
        # Mysql.should_receive(:)
      end
      it 'should created instance' do 
        # sqlite = Sqlite.new(['col1', 'col2'], 'database.db', 'select * from table')
        # expect(sqlite.columns).to eq(['col1', 'col2'])
      end
    end

    context 'instantiate with sources.yml file' do

      # let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }
      # let(:sqlite_attributes) { config.get_source_by_id('sqlite3_game_level_database') }

      # it 'should created instance' do
      #   sqlite_object = Sqlite.new(sqlite_attributes['columns'], sqlite_attributes['file_path'], sqlite_attributes['query'])
      #   expect(sqlite_object.columns).to eq(sqlite_attributes['columns'])
      #   expect(sqlite_object.query).to eq(sqlite_attributes['query'])
      # end

    end

    context 'when authentication failed' do

    end
    
  end

  context "#retrieve_data" do

    let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }

    context "configuration correct" do
      # let(:sqlite_attributes) { config.get_source_by_id('sqlite3_game_level_database') }
      # let(:sqlite_object) { Sqlite.new(sqlite_attributes['columns'], sqlite_attributes['file_path'], sqlite_attributes['query']) }

      # it 'should return array data' do
      #   expected_data = [1, 'dragon_cube', 50]
      #   expect(sqlite_object.retrieve_data.first).to eq(expected_data)
      # end
    end

    context "query incorrect" do
      # let(:sqlite_attributes) { config.get_source_by_id('game_level_db_incorrect') }
      # let(:sqlite_object) { Sqlite.new(sqlite_attributes['columns'], sqlite_attributes['file_path'], sqlite_attributes['query']) }

      # it 'should error' do
      #   expect { sqlite_object.retrieve_data }.to raise_error
      # end
    end
  end
end