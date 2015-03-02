require 'spec_helper'
require 'optimus_prime/destinations/csv'

RSpec.describe OptimusPrime::Destinations::Csv do
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

  def upload(destination)
    input.each { |obj| destination.write obj }
    destination.close
  end

  def download(destination)
    object = s3.get_object bucket: bucket, key: destination.key
    CSV.new object.body, converters: :all
  end

  def test(destination)
    upload destination
    csv = download destination
    header = csv.first
    expect(header).to eq destination.fields
    expect(csv.map { |row| header.zip(row).to_h })
      .to eq input.map { |row| row.select { |k, v| header.include? k } }
  end

  it 'should upload csv to s3' do
    test OptimusPrime::Destinations::Csv.new fields: ['name', 'age'],
                                             bucket: bucket,
                                             key: 'people.csv',
                                             **aws_params
  end

  it 'should upload csv to s3 in chunks' do
    test OptimusPrime::Destinations::Csv.new fields: ['name', 'age'],
                                             bucket: bucket,
                                             key: 'people-chunks.csv',
                                             chunk_size: 5,
                                             **aws_params
  end
end
