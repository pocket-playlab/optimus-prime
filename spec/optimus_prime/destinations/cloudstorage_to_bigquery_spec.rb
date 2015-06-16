require 'spec_helper'
require 'optimus_prime/destinations/cloudstorage_to_bigquery'

RSpec.describe OptimusPrime::Destinations::CloudstorageToBigquery do

  let(:schema) do
    {
      fields: [
        {
          name: 'PlayerID',
          type: 'STRING'
        },
        {
          name: 'Device',
          type: 'STRING'
        },
        {
          name: 'Extras',
          type: 'STRING'
        },
        {
          name: 'Event',
          type: 'STRING'
        }
      ]
    }
  end

  let(:params) do
    {
      table1: ['gs://optimus-prime-test/closeaccount-small.json.gz', 'gs://optimus-prime-test/newuser-small.json.gz'],
      table2: ['gs://optimus-prime-test/login-small.json.gz']
    }
  end

  let(:destination) do
    d = OptimusPrime::Destinations::CloudstorageToBigquery.new(
      client_email: ENV.fetch('GOOGLE_CLIENT_EMAIL', 'test@developer.gserviceaccount.com'),
      private_key: ENV.fetch('GOOGLE_PRIVATE_KEY', File.read('spec/supports/key')),
      project: 'pl-playground',
      dataset: 'json_load',
      schema: schema
    )
    d.logger = Logger.new STDOUT
    d
  end

  it 'runs without errors' do
    destination.write params
    destination.close
  end

end
