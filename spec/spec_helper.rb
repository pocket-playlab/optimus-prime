require 'webmock/rspec'
require 'optimus_prime'

WebMock.disable_net_connect! allow_localhost: true
ENV['AWS_ACCESS_KEY_ID'] = SecureRandom.hex
ENV['AWS_SECRET_ACCESS_KEY'] = SecureRandom.hex
ENV['AWS_REGION'] = 'us-east-1'

RSpec.configure do |config|
  s3 = nil

  config.before :suite do
    system 'rm -r /tmp/s3'
    s3 = spawn 'fakes3 --port 10001 --root /tmp/s3', err: '/dev/null'
    sleep 1
  end

  config.after :suite do
    Process.kill 'INT', s3
    Process.wait s3
  end
end
