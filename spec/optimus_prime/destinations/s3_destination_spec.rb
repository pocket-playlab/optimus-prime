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

  let(:input) { ['s1', 's2', 's3'] }

  before(:each) { s3.create_bucket bucket: params[:bucket] }

  def test
    step.run_with(input.reverse)
    expect(s3.get_object(bucket: params[:bucket], key: params[:key]).body.read).to eq(input.join)
  end

  it('uploads string to s3') { test }

  it 'uploads string to s3 if the chunks is less than the file size' do
    options[:chunk_size] = 1
    test
  end

  it 'uploads string to s3 if the chunks is larger than the file size' do
    options[:chunk_size] = 10000
    test
  end

  it 'pushes bucket and key when it finishes' do
    expect(step.run_with([])).to match_array [{ bucket: params[:bucket], key: params[:key] }]
  end
end
