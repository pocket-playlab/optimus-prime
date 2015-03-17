require 'spec_helper'
require 'optimus_prime/destinations/bigquery'

RSpec.describe OptimusPrime::Destinations::Bigquery do
  let(:resource) do
    {
      'kind' => 'bigquery#table',
      'id' => 'test',
      'tableReference' => {
        'projectId' => 'ppl-analytics',
        'datasetId' => 'test',
        'tableId' => 'test',
      },
      'schema' => {
        'fields' => [
          { 'name' => 'name', 'type' => 'STRING' },
          { 'name' => 'age', 'type' => 'INTEGER' },
        ]
      },
    }
  end

  let(:input) do
    [
      { 'name' => 'Bob',   'age' => 28, 'likes' => 'cheese' },
      { 'name' => 'Alice', 'age' => 34, 'likes' => 'durian' },
      { 'name' => 'Bob',   'age' => 28, 'likes' => 'cheese' },  # duplicate
    ]
  end

  let(:destination) do
    OptimusPrime::Destinations::Bigquery.new(
      client_email: ENV.fetch('GOOGLE_CLIENT_EMAIL', 'test@developer.gserviceaccount.com'),
      private_key: ENV.fetch('GOOGLE_PRIVATE_KEY', File.open('spec/supports/key') { |f| f.read }),
      resource: resource,
      id_field: 'name',
    )
  end

  let(:bigquery) { destination.send :bigquery }

  def delete_table
    destination.send :execute, bigquery.tables.delete, params: { 'tableId' => 'test' }
  rescue
    false  # already deleted
  end

  def create_table
    destination.send :create_table
  end

  def upload
    input.each { |obj| destination.write obj }
    destination.close
  end

  def download
    response = destination.send :execute, bigquery.tabledata.list, params: { 'tableId' => 'test' }
    json = JSON.parse response.body
    json['rows'].map do |row|
      { 'name' => row['f'][0]['v'], 'age' => row['f'][1]['v'].to_i }
    end
  end

  def expected
    input.take(2).map do |row|
      row.select { |k, v| ['name', 'age'].include? k }
    end
  end

  def test
    upload
    # sleep 60  # Needed when running on the real bigquery
    rows = download
    expect(rows).to match_array expected
  end

  context 'table does not exist' do
    it 'should create a table and stream data to it' do
      VCR.use_cassette('bigquery/new-table') do
        delete_table
        test
      end
    end
  end

  context 'table already exists' do
    it 'should stream data to the existing table' do
      VCR.use_cassette('bigquery/table-exists') do
        delete_table
        create_table
        test
      end
    end
  end
end
