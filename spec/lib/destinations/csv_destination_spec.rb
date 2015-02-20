require 'spec_helper'

RSpec.describe CsvDestination do

  aws_params = { endpoint: 'http://localhost:10001/', force_path_style: true }

  let(:s3) { Aws::S3::Client.new aws_params }

  let(:bucket) { 'ppl-csv-test' }

  let(:input) do
    [
      { 'name' => 'Bob',   'age' => 28, 'likes' => 'cheese' },
      { 'name' => 'Alice', 'age' => 34, 'likes' => 'durian' },
    ]
  end

  before :each do
    s3.create_bucket bucket: bucket
  end

  def test_upload(destination)
    input.each { |obj| destination.write obj }
    destination.close
    object = s3.get_object bucket: bucket, key: destination.key
    csv = CSV.new object.body, converters: :all
    header = csv.first
    expect(header).to eq destination.fields
    rows = csv.map { |row| header.zip(row).to_h }
    expect(rows).to eq input.map { |row| row.select { |k, v| header.include? k } }
  end

  it 'should upload csv to s3' do
    destination = CsvDestination.new fields: ['name', 'age'],
                                     bucket: bucket,
                                     key: 'people.csv',
                                     **aws_params
    test_upload destination
  end

  it 'should upload csv to s3 in chunks' do
    destination = CsvDestination.new fields: ['name', 'age'],
                                     bucket: bucket,
                                     key: 'people-chunks.csv',
                                     chunk_size: 5,
                                     **aws_params
    test_upload destination
  end

end
