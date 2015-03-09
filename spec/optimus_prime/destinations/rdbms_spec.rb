require 'spec_helper'
require 'optimus_prime/destinations/rdbms'

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
  let(:table) { :developer_cars }

  let(:destination) do
    OptimusPrime::Destinations::Rdbms.new dsn: dsn, table: table, sql_trace: false
  end

  before do
    db = Sequel.connect(dsn)
    # if you need to debug or trace sql, uncomment following lines
    # db.loggers << Logger.new($stdout)
    # db.sql_log_level = :debug
    db.drop_table? table
    db.create_table table do
      String :name
      String :car
      Integer :horsepower
    end
  end

  def insert_records
    input.each { |record| destination.write record }
    destination.close
  end

  def records_from_db
    db = Sequel.connect(dsn)
    db[table].all
  end

  it 'should upload insert records into database' do
    insert_records
    expect(records_from_db).to eq(input)
  end
end
