require 'spec_helper'
require 'optimus_prime/sources/appsflyer'

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
  let(:table) { db[table_name.to_sym] }

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
      rows = OptimusPrime::Sources::Rdbms.new dsn: dsn, query: 'select * from events'
      rows.each do |row|
        expect(row.keys).to match_array [:platform, :version]
      end
    end

    it 'errors when table not exist' do
      rows = OptimusPrime::Sources::Rdbms.new dsn: dsn, query: 'select * from empty_table'
      expect { rows.each }.to raise_error
    end
  end

end
