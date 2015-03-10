require 'spec_helper'
require 'optimus_prime/sources/local_csv'

RSpec.describe OptimusPrime::Sources::LocalCsv do
  let(:source) do
    OptimusPrime::Sources::LocalCsv.new file_path: 'spec/supports/csv/local_csv_source_sample.csv'
  end

  let(:pipe_source) do
    OptimusPrime::Sources::LocalCsv.new file_path: 'spec/supports/csv/local_csv_source_pipe.csv',
                                        col_sep: '|'
  end

  let(:inexistent_file_source) do
    OptimusPrime::Sources::LocalCsv.new file_path: 'fake/file.csv'
  end

  let(:malformed_source) do
    OptimusPrime::Sources::LocalCsv.new file_path: 'spec/supports/csv/local_csv_src_malformed.csv'
  end

  context 'valid csv' do
    it 'should yield records with default separator (,)' do
      source.each do |record|
        expect(record.keys).to include(
          'FirstName',
          'LastName',
          'Title',
          'ReportsTo.Email',
          'Birthdate',
          'Description',
        )
      end
    end

    it 'should yield records with custom separator (|)' do
      pipe_source.each do |record|
        expect(record.keys).to include(
          'FirstName',
          'LastName',
          'Title',
          'ReportsTo.Email',
          'Birthdate',
          'Description',
        )
      end
    end
  end

  context 'malformed csv' do
    it 'should raise malformed csv' do
      expect { malformed_source.each { |record| } }.to raise_error(CSV::MalformedCSVError)
    end
  end

  context 'Inexistent file' do
    it 'should raise a Errno::ENOENT exception' do
      expect { inexistent_file_source.each { |record| } }.to raise_error(Errno::ENOENT)
    end
  end
end
