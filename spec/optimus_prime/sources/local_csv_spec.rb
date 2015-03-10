require 'spec_helper'
require 'optimus_prime/sources/local_csv'

RSpec.describe OptimusPrime::Sources::LocalCsv do
  let(:file_path) do
    'spec/supports/csv/local_csv_source_sample.csv'
  end

  let(:source) do
    OptimusPrime::Sources::LocalCsv.new file_path: file_path
  end

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
end
