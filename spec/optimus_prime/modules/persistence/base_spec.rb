require 'spec_helper'

RSpec.describe OptimusPrime::Modules::Persistence::Base do

  let(:db) do
    Sequel.connect('sqlite:base_test.db')
  end

  let(:operation_columns) do
    [:id, :pipeline_id, :start_time, :end_time, :status, :error]
  end

  let(:load_job_columns) do
    [:id, :identifier, :job_id, :operation_id, :uris, :category, :status, :start_time, :end_time, :error]
  end

  let(:base) do
    OptimusPrime::Modules::Persistence::Base.new(dsn: 'sqlite:listener_test.db')
  end

  it 'runs the migration when instantiated' do
    expect(base.db['select * from operations'].columns).to eq(operation_columns)
    expect(base.db['select * from load_jobs'].columns).to eq(load_job_columns)
  end

end
