require 'spec_helper'
require 'optimus_prime/destinations/csv'

RSpec.describe OptimusPrime::Destinations::Csv do
  let(:params) do
    {
      fields: ['name', 'age'],
      bucket: 'ppl-csv-test',
      key: 'people.csv',
    }
  end
  let(:options) { { endpoint: 'http://localhost:10001/', force_path_style: true } }
  let(:s3) { Aws::S3::Client.new options }
  let(:step) { OptimusPrime::Destinations::Csv.new(**params.merge(options))}

  let(:input) do
    [
      { 'name' => 'Bob',   'age' => 28, 'likes' => 'cheese' },
      { 'name' => 'Alice', 'age' => 34, 'likes' => 'durian' },
    ]
  end
  let(:output) { [{ "name" => "Bob", "age" => 28 }, { "name" => "Alice", "age" => 34 }] }

  before(:each) { s3.create_bucket bucket: params[:bucket] }

  def parse(csv)
    csv.map { |row| params[:fields].zip(row).to_h }
  end

  def test
    step.run_with(input.dup)
    csv = CSV.new(s3.get_object(bucket: params[:bucket], key: params[:key]).body, converters: :all)
    header = csv.first
    expect(header).to match_array params[:fields]
    expect(parse csv).to match_array output
  end

  it('uploads csv to s3') { test }

  it 'uploads csv to s3 in chunks' do
    options[:chunk_size] = 5
    test
  end

  it 'pushes bucket and key when it finishes' do
    expect(step.run_with([])).to match_array [{ bucket: params[:bucket], key: params[:key] }]
  end
end
