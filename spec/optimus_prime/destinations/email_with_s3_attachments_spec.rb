require 'spec_helper'

RSpec.describe OptimusPrime::Destinations::EmailWithS3Attachments do
  include Mail::Matchers

  let(:s3) { Aws::S3::Client.new(endpoint: 'http://localhost:10001/', force_path_style: true) }
  let(:bucket) { 'test-bucket' }
  let(:obj_key) { 'test-file' }
  let(:step) do
    OptimusPrime::Destinations::EmailWithS3Attachments.new(
      sender: 'test@example.com', recipients: 'a@example.com, b@example.com',
      title: 'Pipeline Report', body: 'Report Details', email_config: { method: :test }
    )
  end

  before :each do
    s3.create_bucket(bucket: bucket)
    s3.put_object(bucket: bucket, key: obj_key, body: 'Lorem Ipsum Dolor Sit Amet')
    Mail::TestMailer.deliveries.clear
  end

  it 'attaches file from s3' do
    expect(Mail::TestMailer.deliveries.length).to eq(0)
    allow(step).to receive(:download).with(bucket: bucket, key: obj_key)
      .and_return(s3.get_object(bucket: bucket, key: obj_key))
    step.run_with([{ bucket: bucket, key: obj_key }])
    expect(Mail::TestMailer.deliveries.length).to eq(1)
    expect(Mail::TestMailer.deliveries.first.attachments.first.filename).to eq(obj_key)
  end
end
