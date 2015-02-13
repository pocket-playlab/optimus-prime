require 'spec_helper'

RSpec.describe EventsCollectorSource do

  let!(:source) do
    EventsCollectorSource.new bucket: 'ppl-events',
                              from: Time.utc(2015, 2, 1),
                              to:   Time.utc(2015, 2, 2)
  end

  let!(:bucket) do
    files = [
      '2015-01-30T04:46:03.232Z.gz',
      '2015-02-01T00:00:00.000Z.gz',
      '2015-02-01T13:21:46.143Z.gz',
      '2015-02-02T01:32:01.312Z.gz',
    ]
    events = files.map do
      [{ 'event_uuid'   => SecureRandom.uuid,
         'install_uuid' => SecureRandom.uuid,
         'app_id'       => 'juicecubes',
         'app_env'      => 'test' }] * 100
    end
    files.zip(events).to_h
  end

  before do
    s3 = source.instance_variable_get(:@s3)
    s3.stub_responses :list_objects, contents: bucket.keys.map { |key| { key: key } }
    bucket.each do |key, events|
      io = StringIO.new
      gz = Zlib::GzipWriter.new io
      events.each { |e| gz.write(JSON.dump(e) + "\n") }
      gz.close
      allow(s3).to receive(:get_object).with(bucket: 'ppl-events', key: key) do
        double body: StringIO.new(io.string)
      end
    end
  end

  it 'should yield events' do
    source.each do |event|
      expect(event.keys).to match_array %w{event_uuid install_uuid app_id app_env}
    end
  end

  it 'should exclude files outside the given date range' do
    expect(source.to_a).to eq bucket.values[1..2].flatten
  end

end
