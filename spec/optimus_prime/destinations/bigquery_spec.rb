require 'spec_helper'
require 'optimus_prime/destinations/bigquery'

RSpec.describe OptimusPrime::Destinations::Bigquery do
  let(:table) do
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
      { 'name' => 'Bob',   'age' => 28 },
      { 'name' => 'Alice', 'age' => 34 },
    ]
  end

  let(:destination) do
    OptimusPrime::Destinations::Bigquery.new(
      client_email: ENV.fetch('GOOGLE_CLIENT_EMAIL', 'test@developer.gserviceaccount.com'),
      private_key: ENV.fetch('GOOGLE_PRIVATE_KEY', File.open('spec/supports/key') { |f| f.read }),
      table: table,
    )
  end

  let(:bigquery) { destination.send :bigquery }

  def delete_table(destination)
    destination.send :execute, bigquery.tables.delete, params: { 'tableId' => 'test' }
  end

  def create_table(destination)
    destination.send :create_table
  end

  def upload(destination)
    input.each { |obj| destination.write obj }
    destination.close
  end

  def download(destination)
    response = destination.send :execute, bigquery.tabledata.list, params: { 'tableId' => 'test' }
    json = JSON.parse response.body
    json['rows'].map do |row|
      { 'name' => row['f'][0]['v'], 'age' => row['f'][1]['v'].to_i }
    end
  end

  def test(destination)
    upload destination
    # sleep 60  # Needed when running on the real bigquery
    rows = download destination
    expect(rows).to match_array input
  end

  context 'table does not exist' do
    it 'should create a table and stream data to it' do
      VCR.use_cassette('bigquery/new-table') do
        delete_table destination
        test destination
      end
    end
  end

  context 'table already exists' do
    it 'should stream data to the existing table' do
      VCR.use_cassette('bigquery/table-exists') do
        delete_table destination
        create_table destination
        test destination
      end
    end
  end
end
