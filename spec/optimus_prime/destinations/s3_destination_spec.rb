require 'spec_helper'

RSpec.describe OptimusPrime::Destinations::S3Destination do
  let(:params) do
    {
      bucket: 'ppl-test',
      key: 'upload.txt',
    }
  end
  let(:options) { { endpoint: 'http://localhost:10001/', force_path_style: true } }
  let(:s3) { Aws::S3::Client.new options }
  let(:step) { OptimusPrime::Destinations::S3Destination.new(**params.merge(options)) }

  let(:input) { ['This file for upload to S3'] }

  before(:each) { s3.create_bucket bucket: params[:bucket] }

  def test
    step.run_with(input.dup)
  end

  it('uploads csv to s3') { test }

  it 'uploads csv to s3 in chunks' do
    options[:chunk_size] = 5
    test
  end

  it 'pushes bucket and key when it finishes' do
    expect(step.run_with([])).to match_array [{ bucket: params[:bucket], key: params[:key] }]
  end
end
