require 'spec_helper'
require 'optimus_prime/destinations/rdbms'
require 'sequel'
require 'sqlite3'

RSpec.describe OptimusPrime::Destinations::Rdbms do
  let(:input) do
    [
      { name:  'Rick',     car:  'C6 Z06',             horsepower:  505  },
      { name:  'Omar',     car:  'Range Rover',        horsepower:  280  },
      { name:  'Prair',    car:  'Toyota Camry',       horsepower:  160  },
      { name:  'M',        car:  'Honda Civic Type R', horsepower:  750  },
      { name:  'Thibault', car:  'Audi S4',            horsepower:  480  },
      { name:  'Tamer',    car:  'Mercedes SLK',       horsepower:  350  },
    ]
  end

  let(:dsn) { 'sqlite://test.db' }
  let(:table) { 'developer_cars' }

  before(:all) do
    db = Sequel.connect('sqlite://test.db')
    #db.loggers << Logger.new($stdout)
    #db.sql_log_level = :debug
    db.drop_table :developer_cars
    db.create_table :developer_cars do
      String :name
      String :car
      Integer :horsepower
    end
  end

  def insert_records_into(destination)
    input.each { |record| destination.write record }
    destination.close
  end

  def get_records_from_db
    db = Sequel.connect(dsn)
    db[table.to_sym].all
  end

  def test(destination)
    insert_records_into destination
    data = get_records_from_db

    expect(data).to eq(input)
  end

  it 'should upload insert records into database' do
    test OptimusPrime::Destinations::Rdbms.new dsn: dsn, table: table, sql_trace: false
  end
end