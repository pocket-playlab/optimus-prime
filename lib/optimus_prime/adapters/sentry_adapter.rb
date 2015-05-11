require 'sentry-raven'

module OptimusPrime
  module Adapters

    class SentryAdapter < BaseAdapter

      def run(&block)
        ::Raven.capture do
          block.call
        end
      end

      private

      def configure_errors_adapter(&block)
        ::Raven.configure do |config|
          block.call(config)
        end
      end

    end

  end
end