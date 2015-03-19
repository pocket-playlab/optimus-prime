require 'mail'

module OptimusPrime
  module Destinations
    class EmailWithS3Attachments < Destination
      def initialize(sender:, recipients:, title:, body:)
        set_default
        @mail = Mail.new do
          from sender
          to recipients
          subject title
          body body
        end
      end

      def set_default
        Mail.defaults do
          delivery_method :smtp, address: ENV.fetch('EMAIL_HOST'),
                                 port: ENV.fetch('EMAIL_PORT'),
                                 domain: ENV.fetch('EMAIL_DOMAIN'),
                                 user_name: ENV.fetch('EMAIL_USERNAME'),
                                 password: ENV.fetch('EMAIL_PASSWORD'),
                                 authentication: 'plain',
                                 enable_starttls_auto: true
        end
      end

      def write(s3_config)
        s3_file = download bucket: s3_config[:bucket], key: s3_config[:key]
        @mail.add_file :filename => s3_config[:key], :content => s3_file.body.read
      end

      def download(bucket:, key:)
        s3 ||= Aws::S3::Client.new
        s3.get_object({ bucket: bucket, key: key })
      end

      def finish
        @mail.deliver!
      end
    end
  end
end
