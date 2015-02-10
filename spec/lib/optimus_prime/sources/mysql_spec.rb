require 'spec_helper'
require 'sequel'

describe MySQL do

  let(:expected_data) { [[1, "itemA", 100], [2, "itemB", 200], [3, "itemC", 2990]] }
  let(:columns_for_test) { {'item_id': 'Integer', 'item_name': 'String', 'item_price': 'Integer'} }

  before :each do
    db = Sequel.connect(:adapter => 'mysql', :user => 'root', :host => 'localhost', :database => 'mysql_juicecubes',:password => 'root')
    db.execute 'DROP TABLE IF EXISTS items'
    File.open('mysql_juicecubes.sql') { |f| db.execute f.read }
  end

  context "#initialize" do
    context "when missing parameter" do
      it { expect { MySQL.new }.to raise_error }
      it { expect { MySQL.new(columns_for_test, 'username', 'password', 'host') }.to raise_error }
      it { expect { MySQL.new(columns_for_test, 'username', 'password', nil, 'db_selected', 'select *') }.to raise_error }
      it { expect { MySQL.new(columns_for_test, 'username', 'password', 'host', nil, 'select *') }.to raise_error }
      it { expect { MySQL.new(columns_for_test, 'username', 'password', nil, 'db_name', 'select *') }.to raise_error }
      it { expect { MySQL.new(nil, 'username', 'password', 'host', 'db_name', 'select *') }.to raise_error('columns required') }
      it { expect { MySQL.new(columns_for_test, 'username', 'password', 'host', 'db_name', nil) }.to raise_error('query required') }
    end

    context "when parameters correctly" do

      it 'should success to create instance and data should be correct' do 
        mysql = MySQL.new(columns_for_test, 'root', 'root', 'localhost', 'mysql_juicecubes', 'select * from items')
        expect(mysql.retrieve_data).to eq(expected_data)
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

      it { expect { MySQL.new(columns_for_test, 'incorrect_username', 'root', 'localhost', 'mysql_juicecubes', 'select * from items') }.to raise_error }
      it { expect { MySQL.new(columns_for_test, 'root', 'incorrect_password', 'localhost', 'mysql_juicecubes', 'select * from items') }.to raise_error }
      it { expect { MySQL.new(columns_for_test, 'root', 'root', 'fake_host', 'mysql_juicecubes', 'select * from items') }.to raise_error }
      it { expect { MySQL.new(columns_for_test, 'root', 'root', 'localhost', 'nil_db', 'select * from items') }.to raise_error }

    end
    
  end

  context "#retrieve_data" do

    let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }

    context "configuration correct" do
      let(:mysql_attr) { config.get_source_by_id('mysql_juicecubes') }
      let(:mysql_instance) { MySQL.new(mysql_attr['columns'], mysql_attr['db_username'], mysql_attr['db_password'], mysql_attr['host'], mysql_attr['db_name'], mysql_attr['query']) }

      it 'should return array data' do
        expect(mysql_instance.retrieve_data).to eq(expected_data)
      end
    end

    context "query incorrect" do
      let(:mysql_attr) { config.get_source_by_id('mysql_juicecubes') }
      let(:mysql_instance) { MySQL.new(mysql_attr['columns'], mysql_attr['db_username'], mysql_attr['db_password'], mysql_attr['host'], mysql_attr['db_name'], 'select * from nil_table') }

      it 'should error' do
        expect { mysql_instance.retrieve_data }.to raise_error
      end
    end
  end
end
