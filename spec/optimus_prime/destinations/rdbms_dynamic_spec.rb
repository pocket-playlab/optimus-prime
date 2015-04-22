require 'spec_helper'
require 'optimus_prime/destinations/rdbms_dynamic'

RSpec.describe OptimusPrime::Destinations::RdbmsDynamic do
  let(:input) do
    [
      { name: 'Rick', color: nil },
      { name: 'Omar', car: 'Range Rover' },
      { name: 'Prair', car: 'Toyota Camry', horsepower: 160 },
      { name: 'M', car: 'Honda Civic Type R', color: 'red' },
      { name: 'Thibault', car: 'Audi S4' },
      { name: 'Tamer', car: 'Mercedes SLK', horsepower: 350, color: 'blue' },
    ]
  end

  let(:output) do
    [
      { id: 1, name: 'Rick', car: nil, horsepower: nil, color: nil },
      { id: 2, name: 'Omar', car: 'Range Rover', horsepower: nil, color: nil },
      { id: 3, name: 'Prair', car: 'Toyota Camry', horsepower: 160, color: nil },
      { id: 4, name: 'M', car: 'Honda Civic Type R', horsepower: nil, color: 'red' },
      { id: 5, name: 'Thibault', car: 'Audi S4', horsepower: nil, color: nil },
      { id: 6, name: 'Tamer', car: 'Mercedes SLK', horsepower: 350, color: 'blue' }
    ]
  end

  let(:dsn) { 'sqlite://test.db' }
  let(:table) { :developer_cars }

  let(:destination) do
    OptimusPrime::Destinations::RdbmsDynamic.new dsn: dsn, table: table, sql_trace: false
  end

  before do
    db = Sequel.connect(dsn)
    # if you need to debug or trace sql, uncomment following lines
    # db.loggers << Logger.new($stdout)
    # db.sql_log_level = :debug
    db.drop_table? table
    db.create_table(table) { primary_key :id }
  end

  it 'writes all record to the database and creates columns as needed' do
    db = Sequel.connect(dsn)
    input.each { |record| destination.write record }
    destination.close
    expect(db[table].all).to eq(output)
  end
end
