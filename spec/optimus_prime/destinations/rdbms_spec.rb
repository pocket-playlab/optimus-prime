require 'spec_helper'

RSpec.describe OptimusPrime::Destinations::Rdbms do
  let(:input) do
    [
      { platform: 'ios', version: '1.0.1' },
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
    OptimusPrime::Destinations::Rdbms.new dsn: dsn, table: table_name,
                                          conditions: "Platform = 'ios'",
                                          sql_trace: false
  end

  let(:destination_with_hash_condition) do
    OptimusPrime::Destinations::Rdbms.new dsn: dsn, table: table_name,
                                          conditions: { version: '1.0.1' },
                                          sql_trace: false
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
      expect(table.where(platform: 'ios').count).to eq 1
      insert_records_with_destination(destination_with_string_condition)
      expect(records_from_db.count).to eq 7
    end

    it 'should upload insert records into database' do
      insert_records_with_destination(destination_with_string_condition)
      expect(records_from_db).to eq(input)
    end
  end

  context 'hash condition' do
    it 'should delete all the records with version 1.0.1' do
      insert_records
      expect(table.where(version: '1.0.1').count).to eq 3
      insert_records_with_destination(destination_with_hash_condition)
      expect(records_from_db.count).to eq 5
    end

    it 'should upload insert records into database' do
      insert_records_with_destination(destination_with_hash_condition)
      expect(records_from_db).to eq(input)
    end
  end
end
