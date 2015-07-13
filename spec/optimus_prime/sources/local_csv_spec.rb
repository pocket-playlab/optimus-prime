require 'spec_helper'

RSpec.describe OptimusPrime::Sources::LocalCsv do
  let(:step) { OptimusPrime::Sources::LocalCsv.new file_path: @file_path, col_sep: col_sep }
  let(:col_sep) { @col_sep || ',' }

  context 'valid csv' do
    it 'should yield records with default separator (,)' do
      @file_path = 'spec/supports/csv/local_csv_source_sample.csv'
      step.run_with.each do |record|
        expect(record.keys).to include('FirstName', 'LastName', 'Title', 'ReportsTo.Email', 'Birthdate', 'Description')
      end
    end

    it 'should yield records with custom separator (|)' do
      @file_path = 'spec/supports/csv/local_csv_source_pipe.csv'
      @col_sep = '|'
      step.run_with.each do |record|
        expect(record.keys).to include('FirstName', 'LastName', 'Title', 'ReportsTo.Email', 'Birthdate', 'Description')
      end
    end
  end

  context 'malformed csv' do
    it 'should raise malformed csv' do
      @file_path = 'spec/supports/csv/local_csv_src_malformed.csv'
      expect { step.run_with }.to raise_error(CSV::MalformedCSVError)
    end
  end

  context 'Inexistent file' do
    it 'should raise a Errno::ENOENT exception' do
      @file_path = 'fake/file.csv'
      expect { step.run_with }.to raise_error(Errno::ENOENT)
    end
  end
end
