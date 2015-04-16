require 'spec_helper'

RSpec.describe OptimusPrime::Destinations::Rdbms do
  let(:input) do
    [
      { platform: 'ios',     version: '1.0.1' },
      { platform: 'android', version: '1.0.1'  },
      { platform: 'android', version: '1.0.2'  },
      { platform: 'android', version: '1.0.1'  }
    ]
  end

  let(:dsn) { 'sqlite://test.db' }
  let(:table_name) { :events }
  let(:db) { Sequel.connect(dsn) }
  let(:table) { db[table_name.to_sym] }

  let(:destination_with_string_condition) do
    dest = OptimusPrime::Destinations::Rdbms.new dsn: dsn, table: table_name, retry_interval: 0.1,
                                          delete_conditions: "platform = 'ios'",
                                          sql_trace: false
    dest.logger = Logger.new(STDERR)
    dest
  end

  let(:destination_with_hash_condition) do
    dest = OptimusPrime::Destinations::Rdbms.new dsn: dsn, table: table_name, retry_interval: 0.1,
                                                           delete_conditions: { version: '1.0.1' },
                                                           sql_trace: false
    dest.logger = Logger.new(STDERR)
    dest
  end

  before do
    db = Sequel.connect(dsn)
    # if you need to debug or trace sql, uncomment following lines
    # db.loggers << Logger.new($stdout)
    # db.sql_log_level = :debug
    db.drop_table? table_name
    db.create_table table_name do
      String :platform
      String :version
    end
  end

  def insert_records
    input.each { |record| table.insert record }
  end

  def insert_records_with_destination(destination)
    input.each { |record| destination.write record }
    destination.close
  end

  def records_from_db
    table.all
  end

  context 'string condition' do
    it 'should delete all the records with ios platform' do
      insert_records
      destination = destination_with_string_condition
      expect(table.where(platform: 'ios').count).to eq 1
      insert_records_with_destination(destination)
      expect(records_from_db.count).to eq 7
    end

    it 'should insert records into database' do
      insert_records_with_destination(destination_with_string_condition)
      expect(records_from_db).to eq(input)
    end

    context 'exception raised' do
      before do
        @dest = destination_with_string_condition
        allow(@dest).to receive(:sleep) {}
        allow(@dest).to receive(:delete_records) do
          allow(@dest).to receive(:delete_records).and_call_original
          raise Sequel::DatabaseConnectionError
        end
      end

      it 'retries when sequel raises a database connection error' do
        insert_records
        insert_records_with_destination(@dest)
        expect(records_from_db.count).to eq 7
      end
    end
  end

  context 'hash condition' do
    it 'should delete all the records with version 1.0.1' do
      insert_records
      destination = destination_with_hash_condition
      expect(table.where(version: '1.0.1').count).to eq 3
      insert_records_with_destination(destination)
      expect(records_from_db.count).to eq 5
    end

    it 'should upload insert records into database' do
      insert_records_with_destination(destination_with_hash_condition)
      expect(records_from_db).to eq(input)
    end

    context 'exception raised' do
      before do
        @dest = destination_with_hash_condition
        allow(@dest).to receive(:sleep) {}
        allow(@dest).to receive(:delete_records) do
          allow(@dest).to receive(:delete_records).and_call_original
          raise Sequel::DatabaseConnectionError
        end
      end

      it 'retries when sequel raises a database connection error' do
        insert_records
        insert_records_with_destination(@dest)
        expect(records_from_db.count).to eq 5
      end
    end
  end

end
