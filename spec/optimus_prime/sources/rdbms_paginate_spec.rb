require 'spec_helper'

describe OptimusPrime::Sources::RdbmsPaginate do
  let(:input) do
    [
      { id: 1, platform: 'ios',     version: '8.1'  },
      { id: 2, platform: 'android', version: '4.4'  },
      { id: 3, platform: 'android', version: '4.2'  },
      { id: 4, platform: 'android', version: '4.3'  },
      { id: 5, platform: 'ios',     version: '8.3'  },
      { id: 6, platform: 'ios',     version: '8.2'  }
    ]
  end

  let(:dsn) { 'sqlite://test.db' }
  let(:table_name) { :devices }
  let(:db) { Sequel.connect(dsn) }
  let(:table) { db[table_name] }

  before do
    db = Sequel.connect(dsn)
    db.drop_table? table_name
    db.create_table table_name do
      String :platform
      String :version
      primary_key :id
    end

    input.each { |record| table.insert record }
  end

  context '#each' do
    it 'matches the same keys of input' do
      step = OptimusPrime::Sources::RdbmsPaginate.new dsn: dsn,
                                                      query: 'select * from devices',
                                                      rows_per_fetch: 2,
                                                      order_field: :id
      step.run_with.each do |row|
        expect(row.keys).to match_array ['id', 'platform', 'version']
      end
    end
  end
end
