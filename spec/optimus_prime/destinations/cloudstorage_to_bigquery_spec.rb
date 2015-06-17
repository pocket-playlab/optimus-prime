require 'spec_helper'
require 'optimus_prime/destinations/cloudstorage_to_bigquery'

RSpec.describe OptimusPrime::Destinations::CloudstorageToBigquery do

  let(:valid_schema) do
    {
      fields: [
        { name: 'PlayerID', type: 'STRING' },
        { name: 'Device',   type: 'STRING' },
        { name: 'Extras',   type: 'STRING' },
        { name: 'Event',    type: 'STRING' }
      ]
    }
  end

  let(:invalid_schema) do
    {
      fields: [
        { name: 'SomeWrongField', type: 'STRING' },
        { name: 'Device',   type: 'STRING' },
        { name: 'Extras',   type: 'STRING' },
        { name: 'Event',    type: 'STRING' }
      ]
    }
  end

  let(:params) do
    {
      table1: ['gs://optimus-prime-test/closeaccount-small.json.gz', 'gs://optimus-prime-test/newuser-small.json.gz'],
      table2: ['gs://optimus-prime-test/login-small.json.gz']
    }
  end

  let(:logger) { Logger.new STDOUT }

  def destination(schema)
    d = OptimusPrime::Destinations::CloudstorageToBigquery.new(
      client_email: ENV.fetch('GOOGLE_CLIENT_EMAIL', 'test@developer.gserviceaccount.com'),
      private_key: ENV.fetch('GOOGLE_PRIVATE_KEY', File.read('spec/supports/key')),
      project: 'pl-playground',
      dataset: 'json_load',
      schema: schema
    )
    d.logger = logger
    d
  end

  it 'runs without errors' do
    d = destination(valid_schema)
    VCR.use_cassette('cloudstorage_to_bigquery/run') do
      d.write(params)
      d.close
    end
  end

  it 'raises an exception for a wrong schema' do
    d = destination(invalid_schema)
    VCR.use_cassette('cloudstorage_to_bigquery/fail') do
      expect{d.write(params)}.to raise_error(OptimusPrime::Destinations::CloudstorageToBigquery::LoadJob::LoadJobError)
      d.close
    end
  end


end
