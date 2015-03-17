require 'mail'

module OptimusPrime
  module Destinations
    class EmailWithAttachments < Destination
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
                                 port: 587,
                                 domain: ENV.fetch('EMAIL_DOMAIN'),
                                 user_name: ENV.fetch('EMAIL_USERNAME'),
                                 password: ENV.fetch('EMAIL_PASSWORD'),
                                 authentication: 'plain',
                                 enable_starttls_auto: true
        end
      end

      def write(file_path)
        @mail.add_file(file_path)
      end

      def finish
        @mail.deliver!
      end
    end
  end
end
