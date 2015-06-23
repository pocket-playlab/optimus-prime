require 'spec_helper'
require 'optimus_prime/modules/persistence/listener'

RSpec.describe OptimusPrime::Modules::Persistence::Listener do

  let(:db) do
    Sequel.connect('sqlite:listener_test.db')
  end

  let(:operation_columns) do
    [:id, :pipeline_id, :start_time, :end_time, :status, :error]
  end

  let(:load_job_columns) do
    [:id, :identifier, :job_id, :operation_id, :uris, :category, :status, :start_time, :end_time, :error]
  end

  let(:listener) do
    OptimusPrime::Modules::Persistence::Listener.new(dsn: 'sqlite:listener_test.db')
  end

  it 'runs the migration when instantiated' do
    expect(listener.db['select * from operations'].columns).to eq(operation_columns)
    expect(listener.db['select * from load_jobs'].columns).to eq(load_job_columns)
  end

end
