require 'spec_helper'

RSpec.describe OptimusPrime::Destinations::EmailWithS3Attachments do
  include Mail::Matchers

  let(:options) { { endpoint: 'http://localhost:10001/', force_path_style: true } }

  let(:s3) { Aws::S3::Client.new options }

  let(:bucket) { 'ppl-csv-test' }

  let(:destination) do
    OptimusPrime::Destinations::Csv.new fields: ['name', 'age'],
                                        bucket: bucket,
                                        key: 'people.csv',
                                        **options
  end

  let(:input) do
    [
      { 'name' => 'Bob',   'age' => 28, 'likes' => 'cheese' },
      { 'name' => 'Alice', 'age' => 34, 'likes' => 'durian' },
    ]
  end

  let(:email_config) { { method: :test } }

  def upload
    input.each { |obj| destination.write obj }
    destination.close
  end

  def s3_object
    s3.get_object(bucket: bucket, key: destination.key)
  end

  def s3_mockup_step
    upload
    csv = CSV.new s3_object.body, converters: :all
    header = csv.first
    expect(header).to eq destination.fields
  end

  before :each do
    s3.create_bucket bucket: bucket
    s3_mockup_step
    Mail::TestMailer.deliveries.clear
  end

  it 'should attach file from s3' do
    expect(Mail::TestMailer.deliveries.length).to eq(0)

    @email = OptimusPrime::Destinations::EmailWithS3Attachments.new(
      sender: 'analytics@playlab.com',
      recipients: 'a@playlab.com, b@playlab.com',
      title: 'analytics report',
      body: 'report detail',
      email_config: email_config)
    @email.stub(:download).with(bucket: bucket, key: destination.key) { s3_object }
    @email.write(bucket: bucket, key: destination.key)
    @email.finish

    expect(Mail::TestMailer.deliveries.length).to eq(1)
    expect(Mail::TestMailer.deliveries.first.attachments.first.filename).to eq(destination.key)
  end
end
