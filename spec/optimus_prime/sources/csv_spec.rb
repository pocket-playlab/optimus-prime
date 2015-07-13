require 'spec_helper'

RSpec.describe OptimusPrime::Sources::Csv do
  let(:bucket) { 'ppl-csv' }
  let(:aws_params) { { endpoint: 'http://localhost:10001/', force_path_style: true } }

  let(:files) do
    ['2015-01-30T04:46:03.232Z.gz', '2015-02-01T00:00:00.000Z.gz',
     '2015-02-01T13:21:46.143Z.gz', '2015-02-02T01:32:01.312Z.gz',]
  end

  let(:events) do
    files.map do
      100.times.map do
        { 'event_uuid' => SecureRandom.uuid, 'install_uuid' => SecureRandom.uuid,
          'app_id' => 'juicecubes', 'app_env' => 'test' }
      end
    end
  end

  let(:step) do
    OptimusPrime::Sources::Csv.new(
      bucket: bucket, from: Time.utc(2015, 2, 1), to: Time.utc(2015, 2, 2), **aws_params)
  end

  before :each do
    client = Aws::S3::Client.new aws_params
    client.create_bucket bucket: bucket
    files.zip(events).each do |key, evts|
      csv_string = CSV.generate do |csv|
        csv << ['event_uuid', 'install_uuid', 'app_id', 'app_env']
        evts.each { |e| csv << e.values }
      end
      client.put_object bucket: bucket, key: key, body: csv_string
    end
  end

  it 'should yield events' do
    step.run_with.each { |event| expect(event.keys).to match_array %w{event_uuid install_uuid app_id app_env} }
  end

  it 'should exclude files outside the given date range' do
    expect(step.run_with.to_a).to match_array events[1..2].flatten
  end
end
