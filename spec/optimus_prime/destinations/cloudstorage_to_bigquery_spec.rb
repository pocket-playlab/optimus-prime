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

  class Listener
    def load_job_failed(job, e)
    end
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
      l = Listener.new
      expect(l).to receive(:load_job_failed).twice
      d.subscribe(l)
      d.write(params)
      d.close
    end
  end

  describe 'persistence' do
    let(:base) do
      OptimusPrime::Modules::Persistence::Base.new(dsn: 'sqlite:listener_test.db')
    end

    let(:listener) { base.listener }

    describe 'existing job' do
      it 'does not re-run an existing finished job' do
      end

      it 're-run an existing failed job' do
      end

      it 're-run an existing failed job' do
      end

      it 'does not do anything if persistence is not enabled' do
      end
    end

    describe 'started' do
      it 'receives the load job started event' do
        VCR.use_cassette('cloudstorage_to_bigquery/started_event') do
          d = destination(valid_schema)
          d.subscribe(listener)
          expect(listener).to receive(:load_job_started).twice
          d.write(params)
          d.close
        end
      end
    end

    describe 'finished' do
      it 'receives the load job finished event' do
        VCR.use_cassette('cloudstorage_to_bigquery/finished_event') do
          d = destination(valid_schema)
          d.subscribe(listener)
          listener.pipeline_started(OptimusPrime::Pipeline.new({}, 'super pipline'))
          expect(listener).to receive(:load_job_finished).twice
          d.write(params)
          d.close
        end
      end

      it 'updates the load job in DB when finished' do
        VCR.use_cassette('cloudstorage_to_bigquery/finished_save') do
          d = destination(valid_schema)
          d.subscribe(listener)
          listener.pipeline_started(OptimusPrime::Pipeline.new({}, 'super pipline'))
          d.write(params)
          d.close
          load_job = base.db[:load_jobs].where(identifier: 'gs://optimus-prime-test/closeaccount-small.json.gz').first
          expect(load_job[:status]).to eq 'finished'
        end
      end
    end

    describe 'failed' do
      it 'receives the load job failed event' do
        VCR.use_cassette('cloudstorage_to_bigquery/failed_event') do
          expect(listener).to receive(:load_job_failed).twice
          d = destination(invalid_schema)
          d.subscribe(listener)
          listener.pipeline_started(OptimusPrime::Pipeline.new({}, 'super pipline'))
          d.write(params)
          d.close
        end
      end

      it 'updates the load job in DB when failed' do
        VCR.use_cassette('cloudstorage_to_bigquery/failed_save') do
          d = destination(invalid_schema)
          d.subscribe(listener)
          listener.pipeline_started(OptimusPrime::Pipeline.new({}, 'super pipline'))
          d.write(params)
          d.close
          load_job = base.db[:load_jobs].where(identifier: 'gs://optimus-prime-test/closeaccount-small.json.gz').first
          expect(load_job[:status]).to eq 'failed'
        end
      end
    end
  end
end
