require 'spec_helper'
require 'optimus_prime/sources/events_collector'

RSpec.describe OptimusPrime::Sources::EventsCollector do
  bucket = 'ppl-events'

  files = [
    '2015-01-30T04:46:03.232Z.gz',
    '2015-02-01T00:00:00.000Z.gz',
    '2015-02-01T13:21:46.143Z.gz',
    '2015-02-02T01:32:01.312Z.gz',
  ]

  events = files.map do
    100.times.map do
      { 'event_uuid'   => SecureRandom.uuid,
        'install_uuid' => SecureRandom.uuid,
        'app_id'       => 'juicecubes',
        'app_env'      => 'test' }
    end
  end

  aws_params = { endpoint: 'http://localhost:10001/', force_path_style: true }

  let(:source) do
    src = OptimusPrime::Sources::EventsCollector.new bucket: bucket,
                                               from: Time.utc(2015, 2, 1),
                                               to:   Time.utc(2015, 2, 2),
                                               **aws_params
    src.logger = Logger.new(STDERR)
    src
  end

  before :all do
    client = Aws::S3::Client.new aws_params
    client.create_bucket bucket: bucket
    files.zip(events).each do |key, evts|
      io = StringIO.new
      gz = Zlib::GzipWriter.new io
      evts.each { |e| gz.write(JSON.dump(e) + "\n") }
      gz.close
      client.put_object bucket: bucket, key: key, body: io.string
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

  context 'invalid parse' do
    let(:invalid_json) { 'test' }
    it 'should skip the record' do
      expect(source.logger).to receive(:error).once
      source.parse(invalid_json)
    end
  end
end
