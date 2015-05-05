module OptimusPrime
  module Adapters

    class SentryAdapter

      def run(&block)
        Raven.capture do
          block.call
        end
      end

      private

      def configure_errors_adapter(&block)
        Raven.configure(block)
      end

    end

  end
end