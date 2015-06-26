require 'spec_helper'
require 'optimus_prime/destinations/cloudstorage_to_bigquery'

RSpec.describe OptimusPrime::Destinations::CloudstorageToBigquery do
  let(:params) do
    {
      table1: [
        'gs://optimus-prime-test/closeaccount-small.json.gz',
        'gs://optimus-prime-test/newuser-small.json.gz'
      ],
      table2: ['gs://optimus-prime-test/login-small.json.gz']
    }
  end

  let(:destination) do
    destination = OptimusPrime::Destinations::CloudstorageToBigquery.new(
      client_email: ENV.fetch('GOOGLE_CLIENT_EMAIL', 'test@developer.gserviceaccount.com'),
      private_key: ENV.fetch('GOOGLE_PRIVATE_KEY', File.read('spec/supports/key')),
      project: 'pl-playground',
      dataset: 'json_load',
      schema: schema
    )
    destination.logger = Logger.new(STDOUT)
    destination
  end

  context 'when supplied schema matches data schema' do
    let(:schema) do
      {
        fields: [
          { name: 'PlayerID', type: 'STRING' },
          { name: 'Device',   type: 'STRING' },
          { name: 'Extras',   type: 'STRING' },
          { name: 'Event',    type: 'STRING' }
        ]
      }
    end

    it 'runs successfully' do
      VCR.use_cassette('cloudstorage_to_bigquery/correct-schema') do
        expect do
          destination.write(params)
          destination.close
        end.to_not raise_error
      end
    end
  end

  context 'when supplied schema does not match data schema' do
    let(:schema) do
      {
        fields: [
          { name: 'SomeWrongField', type: 'STRING' },
          { name: 'Device',   type: 'STRING' },
          { name: 'Extras',   type: 'STRING' },
          { name: 'Event',    type: 'STRING' }
        ]
      }
    end

    it 'raises an error' do
      VCR.use_cassette('cloudstorage_to_bigquery/bad-schema') do
        expect { destination.write(params) }.to raise_error(
          OptimusPrime::Destinations::CloudstorageToBigquery::LoadJob::LoadJobError
        )
        destination.close
      end
    end
  end
end
