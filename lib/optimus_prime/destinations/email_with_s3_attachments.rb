require 'mail'

module OptimusPrime
  module Destinations
    class EmailWithS3Attachments < Destination
      def initialize(sender:, recipients:, title:, body:, email_config:)
        mail_settings email_config.with_indifferent_access
        @mail = Mail.new do
          from sender
          to recipients
          subject title
          body body
        end
      end

      def mail_settings(email_config)
        Mail.defaults do
          delivery_method email_config[:method] || :smtp,
                          address: email_config[:address],
                          port: email_config[:port],
                          domain: email_config[:domain],
                          user_name: email_config[:user_name],
                          password: email_config[:password],
                          authentication: 'plain',
                          enable_starttls_auto: true
        end
      end

      def write(s3_config)
        s3_file = download bucket: s3_config[:bucket], key: s3_config[:key]
        @mail.add_file filename: s3_config[:key], content: s3_file.body.read
      end

      def download(bucket:, key:)
        s3 ||= Aws::S3::Client.new
        s3.get_object(bucket: bucket, key: key)
      end

      def finish
        @mail.deliver
      end
    end
  end
end
