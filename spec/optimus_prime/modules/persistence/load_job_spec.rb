require 'spec_helper'

RSpec.describe OptimusPrime::Modules::Persistence::LoadJob do
  let(:db) { Sequel.connect('sqlite:load_job_test.db') }
  let(:table) { db[:load_jobs] }

  let(:operation) do
    OptimusPrime::Modules::Persistence::Operation.new(db)
  end

  let(:load_job) do
    OptimusPrime::Modules::Persistence::LoadJob.new(db)
  end

  let(:operation_id) do
    operation_params = { pipeline_id: 'super_pipeline',
                         start_time: Time.now,
                         status: 'started' }
    operation.create(operation_params)
  end

  let(:params) do
    { identifier: '/super_game/ios/1.0.0/20150315/050000/1.json.gz',
      job_id: 'google_bigquery_job_id',
      operation_id: operation_id,
      uris: '/super_game/ios/1.0.0/20150315/050000/1.json.gz,
             /super_game/ios/1.0.0/20150315/050000/2.json.gz,
             /super_game/ios/1.0.0/20150315/050000/3.json.gz',
      category: 'super_game_ios_1_0_0',
      status: 'started',
      start_time: Time.now }
  end

  before(:each) do
    Sequel::Migrator.run(db, 'migrations')
  end

  after(:each) do
    Sequel::Migrator.run(db, 'migrations', target: 0)
  end

  it 'creates a load job' do
    id = load_job.create(params)
    expect(table.count).to eq 1
    expect(table.where(id: id).first[:identifier]).to eq '/super_game/ios/1.0.0/20150315/050000/1.json.gz'
  end

  it 'updates a load job' do
    id = load_job.create(params)
    load_job.update(identifier: '/super_game/ios/1.0.0/20150315/050000/1.json.gz', job_id: 'test')
    expect(table.where(id: id).first[:job_id]).to eq 'test'
  end
end
