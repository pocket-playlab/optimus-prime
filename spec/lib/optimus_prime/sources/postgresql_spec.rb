require 'spec_helper'
require 'sequel'

describe PostgreSQL do

  let(:columns_test) { { 'col1': 'String', 'col2': 'String' } }

  context "#initialize" do

    context "when missing parameter" do
      it { expect { PostgreSQL.new }.to raise_error }
      it { expect { PostgreSQL.new(columns_test, 'username', 'password', 'host') }.to raise_error }
      it { expect { PostgreSQL.new(columns_test, nil, 'username', 'password', 'db_selected', 'select *') }.to raise_error('cannot connect database') }
      it { expect { PostgreSQL.new(columns_test, 'username', 'password', 'host', nil, 'select *') }.to raise_error('cannot connect database') }
      it { expect { PostgreSQL.new(columns_test, 'username', 'password', nil, 'db_name', 'select *') }.to raise_error('cannot connect database') }
      it { expect { PostgreSQL.new(nil, 'host', 'username', 'password', 'db_name', 'select *') }.to raise_error('columns required') }
      it { expect { PostgreSQL.new(columns_test, 'username', 'password', 'host', 'db_name', nil) }.to raise_error('query required') }
    end

    context "when parameters correctly" do

      it 'should created instance' do 
        postgres = PostgreSQL.new(columns_test, 'postgres', 'root', 'localhost', 'postgres_cubes', 'select * from cubes')
        expect(postgres.class).to eq(PostgreSQL)
      end

    end

    context 'when authentication failed' do

      it { expect { PostgreSQL.new(columns_test, 'incorrect_username', 'root', 'localhost', 'postgres_cubes', 'select * from items') }.to raise_error }
      it { expect { PostgreSQL.new(columns_test, 'postgres', 'incorrect_password', 'localhost', 'postgres_cubes', 'select * from items') }.to raise_error }
      it { expect { PostgreSQL.new(columns_test, 'postgres', 'root', 'fake_host', 'postgres_cubes', 'select * from items') }.to raise_error }
      it { expect { PostgreSQL.new(columns_test, 'postgres', 'root', 'localhost', 'nil_db', 'select * from items') }.to raise_error }

    end

    context 'instantiate with sources.yml file' do

      let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }
      let(:postgres_attributes) { config.get_source_by_id('postgres_cubes') }

      it 'should created instance' do
        columns = postgres_attributes['columns']
        db_username = postgres_attributes['db_username']
        db_password = postgres_attributes['db_password']
        host = postgres_attributes['host']
        query = postgres_attributes['query']
        db_name = postgres_attributes['db_name']

        postgres_instance = PostgreSQL.new(columns, db_username, db_password, host, db_name, query)
        expect(postgres_instance.columns).to eq(postgres_attributes['columns'])
        expect(postgres_instance.query).to eq(postgres_attributes['query'])
      end

    end
    
  end

  context "#retrieve_data" do

    let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }

    context "configuration correct" do
      let(:postgres_attributes) { config.get_source_by_id('postgres_cubes') }
      let(:postgres_instance) { PostgreSQL.new(postgres_attributes['columns'], postgres_attributes['db_username'], postgres_attributes['db_password'], postgres_attributes['host'], postgres_attributes['db_name'], postgres_attributes['query']) }

      it 'should return array data' do
        expected_data = [1, 'jungle']
        expect(postgres_instance.retrieve_data.first).to eq(expected_data)
      end
    end

    context "query incorrect" do
      let(:postgres_attr) { config.get_source_by_id('postgres_cubes') }
      let(:postgres_instance) { postgres.new(postgres_attr['columns'], postgres_attr['db_username'], postgres_attr['db_password'], postgres_attr['host'], postgres_attr['db_name'], 'select * from nil_table') }

      it 'should error' do
        expect { postgres_instance.retrieve_data }.to raise_error
      end
    end

  end
end
