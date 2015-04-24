require 'spec_helper'
require 'optimus_prime/destinations/rdbms_writer'

RSpec.describe OptimusPrime::Destinations::RdbmsWriter do
  let(:input) do
    [
      { name: 'Rick',     car: 'C6 Z06',             horsepower: 505 },
      { name: 'Omar',     car: 'Range Rover',        horsepower: 280 },
      { name: 'Prair',    car: 'Toyota Camry',       horsepower: 160 },
      { name: 'M',        car: 'Honda Civic Type R', horsepower: 750 },
      { name: 'Thibault', car: 'Audi S4',            horsepower: 480 },
      { name: 'Tamer',    car: 'Mercedes SLK',       horsepower: 350 },
      { name: 'Tamer',    car: 'Mercedes SLK',       horsepower: 350 },
    ]
  end

  let(:output) do
    input.uniq { |record| record[:name] }
  end

  let(:dsn) { 'sqlite://test.db' }
  let(:table) { :developer_cars }

  let(:destination) do
    OptimusPrime::Destinations::RdbmsWriter.new(dsn: dsn, table: table,
                                                max_retries: 4, sql_trace: false).tap do |d|
      d.logger = Logger.new(STDERR)
    end
  end

  before do
    db = Sequel.connect(dsn)
    # if you need to debug or trace sql, uncomment following lines
    # db.loggers << Logger.new($stdout)
    # db.sql_log_level = :debug
    db.drop_table? table
    db.create_table table do
      String :name, unique: true
      String :car
      Integer :horsepower
    end
  end

  def insert_records(dest)
    input.each { |record| dest.write record }
    destination.close
  end

  def records_from_db
    db = Sequel.connect(dsn)
    db[table].all
  end

  def stub_run_block(dest, attemps)
    i = 0
    allow(dest).to receive(:run_block) do
      allow(dest).to receive(:run_block).and_call_original if i == attemps
      i += 1
      raise Sequel::DatabaseConnectionError
    end
  end

  it 'should upload insert records into database' do
    insert_records(destination)
    expect(records_from_db).to eq(output)
  end

  context 'exception raised' do
    before do
      @dest = destination
      allow(@dest).to receive(:sleep) {}
    end

    it 'retries when sequel raises a database connection error' do
      stub_run_block(@dest, 2)
      insert_records(@dest)
      expect(records_from_db).to eq(output)
    end

    it 'fails if the number of attempts is over max_retries' do
      stub_run_block(@dest, 3)
      expect { insert_records(@dest) }.to raise_error Sequel::DatabaseConnectionError
    end
  end
end
