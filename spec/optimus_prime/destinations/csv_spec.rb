require 'spec_helper'
require 'optimus_prime/destinations/csv'

RSpec.describe OptimusPrime::Destinations::Csv do
  let(:options) { { endpoint: 'http://localhost:10001/', force_path_style: true } }

  let(:s3) { Aws::S3::Client.new options }

  let(:bucket) { 'ppl-csv-test' }

  let(:destination) do
    OptimusPrime::Destinations::Csv.new fields: ['name', 'age'],
                                        bucket: bucket,
                                        key: 'people.csv',
                                        **options
  end

  let(:input) do
    [
      { 'name' => 'Bob',   'age' => 28, 'likes' => 'cheese' },
      { 'name' => 'Alice', 'age' => 34, 'likes' => 'durian' },
    ]
  end

  before :each do
    s3.create_bucket bucket: bucket
  end

  def upload
    input.each { |obj| destination.write obj }
    destination.close
  end

  def download
    object = s3.get_object bucket: bucket, key: destination.key
    CSV.new object.body, converters: :all
  end

  def expected
    input.map do |row|
      row.select { |k, v| destination.fields.include? k }
    end
  end

  def parse(csv)
    csv.map { |row| destination.fields.zip(row).to_h }
  end

  def test
    upload
    csv = download
    header = csv.first
    expect(header).to eq destination.fields
    expect(parse csv).to eq expected
  end

  it 'should upload csv to s3' do
    test
  end

  it 'should upload csv to s3 in chunks' do
    options[:chunk_size] = 5
    test
  end
end
