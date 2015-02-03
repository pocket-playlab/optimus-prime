require 'spec_helper'
require 'sequel'

describe MySQL do
  context "#initialize" do

    context "when missing parameter" do
      it { expect { MySQL.new }.to raise_error }
      it { expect { MySQL.new(['col1', 'col2'], 'username', 'password', 'host') }.to raise_error }
      it { expect { MySQL.new(['col1', 'col2'], nil, 'username', 'password', 'db_selected', 'select *') }.to raise_error('cannot connect database') }
      it { expect { MySQL.new(['col1', 'col2'], 'username', 'password', 'host', nil, 'select *') }.to raise_error('cannot connect database') }
      it { expect { MySQL.new(['col1', 'col2'], 'username', 'password', nil, 'db_name', 'select *') }.to raise_error('cannot connect database') }
      it { expect { MySQL.new(nil, 'host', 'username', 'password', 'db_name', 'select *') }.to raise_error('columns required') }
      it { expect { MySQL.new(['col1', 'col2'], 'username', 'password', 'host', 'db_name', nil) }.to raise_error('query required') }
    end

    context "when parameters correctly" do

      it 'should created instance' do 
        mysql = MySQL.new(['col1', 'col2'], 'root', 'root', 'localhost', 'mysql_juicecubes', 'select * from items')
        expect(mysql.class).to eq(MySQL)
      end

    end

    context 'instantiate with sources.yml file' do

      let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }
      let(:mysql_attributes) { config.get_source_by_id('mysql_juicecubes') }

      it 'should created instance' do
        columns = mysql_attributes['columns']
        db_username = mysql_attributes['db_username']
        db_password = mysql_attributes['db_password']
        host = mysql_attributes['host']
        query = mysql_attributes['query']
        db_name = mysql_attributes['db_name']

        mysql_instance = MySQL.new(columns, db_username, db_password, host, db_name, query)
        expect(mysql_instance.columns).to eq(mysql_attributes['columns'])
        expect(mysql_instance.query).to eq(mysql_attributes['query'])
      end

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