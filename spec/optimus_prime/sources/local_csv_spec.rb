require 'spec_helper'
require 'optimus_prime/sources/local_csv'

RSpec.describe OptimusPrime::Sources::LocalCsv do
  let(:file_path) { 'spec/supports/csv/local_csv_source_sample.csv' }
  let(:pipe_file_path) { 'spec/supports/csv/local_csv_source_pipe.csv' }
  let(:malformed_file_path) { 'spec/supports/csv/local_csv_source_malformed.csv' }

  let(:source) do
    OptimusPrime::Sources::LocalCsv.new file_path: file_path
  end

  let(:pipe_source) do
    OptimusPrime::Sources::LocalCsv.new file_path: pipe_file_path, col_sep: '|'
  end

  let(:malformed_source) do
    OptimusPrime::Sources::LocalCsv.new file_path: malformed_file_path
  end

  context 'with default separator' do
    it 'should yield records' do
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

    it 'should raise malformed csv' do
      expect { malformed_source.each { |record| } }.to raise_error(CSV::MalformedCSVError)
    end
  end

  context 'with custom separator' do
    it 'should yield records' do
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
end
