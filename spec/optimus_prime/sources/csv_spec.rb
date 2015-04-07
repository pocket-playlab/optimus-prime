require 'spec_helper'
require 'optimus_prime/sources/events_collector'

RSpec.describe OptimusPrime::Sources::Csv do
  bucket = 'ppl-csv'

  files = [
    '2015-01-30T04:46:03.232Z.csv',
    '2015-02-01T00:00:00.000Z.csv',
    '2015-02-01T13:21:46.143Z.csv',
    '2015-02-02T01:32:01.312Z.csv',
  ]

  events = files.map do
    3.times.map do
      { 'event_uuid'   => SecureRandom.uuid,
        'install_uuid' => SecureRandom.uuid,
        'app_id'       => 'juicecubes',
        'app_env'      => 'test' }
    end
  end

  aws_params = { endpoint: 'http://localhost:10001/', force_path_style: true }

  let(:source) do
    OptimusPrime::Sources::Csv.new bucket: bucket,
                                   from: Time.utc(2015, 2, 1),
                                   to:   Time.utc(2015, 2, 2),
                                   **aws_params
  end

  before :all do
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
    source.each do |event|
      expect(event.keys).to match_array %w{event_uuid install_uuid app_id app_env}
    end
  end

  it 'should exclude files outside the given date range' do
    expect(source.to_a).to match_array events[1..2].flatten
  end
end
