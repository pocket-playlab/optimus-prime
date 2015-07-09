require 'spec_helper'

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
  let!(:output) { input.uniq { |record| record[:name] } }
  let(:dsn) { 'sqlite://test.db' }
  let(:table) { :developer_cars }
  let(:step) do
    OptimusPrime::Destinations::RdbmsWriter.new(dsn: dsn, table: table, max_retries: 4,
                                                chunk_size: 10, sql_trace: false)
      .suppress_log
  end

  def stub_run_block(attemps)
    i = 0
    allow(step).to receive(:run_block) do
      allow(step).to receive(:run_block).and_call_original if i == attemps
      i += 1
      raise Sequel::DatabaseConnectionError
    end
  end

  def run_test
    step.run_with(input)
    db = Sequel.connect(dsn)
    expect(db[table].all).to match_array output
  end

  before :each do
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

  context 'exception handling' do
    before(:each) { allow(step).to receive(:sleep) {} }

    it 'retries when sequel raises a database connection error' do
      stub_run_block(2)
      run_test
    end

    it 'fails if the number of attempts is over max_retries' do
      stub_run_block(3)
      expect { step.run_with(input) }.to raise_error Sequel::DatabaseConnectionError
    end
  end

  context 'when records.count < chunk_size' do
    it 'inserts records into database' do
      run_test
    end
  end

  context 'when records.count = chunk_size' do
    it 'inserts records into database' do
      step.instance_variable_set('@chunk_size', input.count)
      run_test
    end
  end

  context 'when records.count > chunk.size' do
    it 'inserts records into database' do
      step.instance_variable_set('@chunk_size', 4)
      run_test
    end
  end
end
