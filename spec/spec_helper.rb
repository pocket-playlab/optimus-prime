require 'webmock/rspec'
require './lib/optimus_prime'

WebMock.disable_net_connect!(allow_localhost: true)
Aws.config[:stub_responses] = true
ENV['AWS_ACCESS_KEY_ID'] = SecureRandom.hex
ENV['AWS_SECRET_ACCESS_KEY'] = SecureRandom.hex
