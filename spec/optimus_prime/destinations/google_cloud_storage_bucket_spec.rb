require 'spec_helper'
require 'optimus_prime/destinations/google_cloud_storage_bucket'

RSpec.describe OptimusPrime::Destinations::GoogleCloudStorageBucket do
  let(:bucket) { 'optimus-prime-test' }
  let(:options) { { base_local_path: 'spec/supports/gcsbucket/samples' } }

  let(:input) do
    [
      { category: 'signins', file: 'login-small.json.gz' },
      { category: 'signups', file: 'nested/newuser-small.json.gz' },
      { category: 'logoffs', file: 'deeply/nested/closeaccount-small.json.gz' }
    ]
  end

  let(:step) do
    OptimusPrime::Destinations::GoogleCloudStorageBucket.new(
      client_email: ENV.fetch('GOOGLE_CLIENT_EMAIL', 'test@developer.gserviceaccount.com'),
      private_key: ENV.fetch('GOOGLE_PRIVATE_KEY', File.read('spec/supports/key')),
      bucket: bucket,
      options: options
    ).log_to(Logger.new(STDOUT))
  end

  context 'when network is stable' do
    it 'uploads all files successfully' do
      VCR.use_cassette('gcsbucket/success', preserve_exact_body_bytes: true) do
        cleaned_output = step.run_with(input.dup).map do |record|
          { category: record[:category], file: record[:file].name }
        end
        expect(cleaned_output).to match_array input
      end
    end
  end

  context 'when network is not stable' do
    it 'fails to upload the files' do
      VCR.use_cassette('gcsbucket/failure', preserve_exact_body_bytes: true) do
        stub_request(:post, %r(https://www.googleapis.com/upload/storage/v1/b/optimus-prime-test/o.*))
          .to_timeout.times(20)
        expect { step.run_and_raise(input) }.to raise_error
      end
    end
  end
end
