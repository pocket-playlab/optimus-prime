require 'spec_helper'

describe OptimusPrime::Sources::Rdbms do
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
  let(:table) { db[table_name] }

  before do
    db = Sequel.connect(dsn)
    db.drop_table? table_name
    db.create_table table_name do
      String :platform
      String :version
    end

    input.each { |record| table.insert record }
  end

  context '#each' do
    it 'matches the same keys of input' do
      step = OptimusPrime::Sources::Rdbms.new dsn: dsn, query: 'select * from events'
      step.run_with.each do |row|
        expect(row.keys).to match_array ['platform', 'version']
      end
    end

    it 'errors when table not exist' do
      step = OptimusPrime::Sources::Rdbms.new dsn: dsn, query: 'select * from empty_table'
      expect { step.run_with }.to raise_error
    end
  end
end
