require 'spec_helper'

RSpec.describe OptimusPrime::Modules::Persistence::Operation do

  let(:db) { Sequel.connect('sqlite:operation_test.db') }
  let(:table) { db[:operations] }

  let(:operation) do
    OptimusPrime::Modules::Persistence::Operation.new(db)
  end

  let(:params) do
    { pipeline_id: 'super_pipeline',
     start_time: Time.now,
     status: 'started' }
  end

  before(:each) do
    Sequel::Migrator.run(db, 'migrations')
  end

  after(:each) do
    Sequel::Migrator.run(db, 'migrations', :target => 0)
  end

  it 'creates an operation' do
    id = operation.create(params)
    expect(table.count).to eq 1
    expect(table.where(id: id).first[:pipeline_id]).to eq 'super_pipeline'
  end

  it 'updates an operation' do
    id = operation.create(params)
    operation.update({id: id, pipeline_id: 'lame_pipeline'})
    expect(table.where(id: id).first[:pipeline_id]).to eq 'lame_pipeline'
  end

end
