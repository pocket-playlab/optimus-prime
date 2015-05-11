module OptimusPrime
  module Adapters

    class BaseAdapter

      def initialize(options)
        configure_errors_adapter do |config|
          options.each do |key, value|
            config.send("#{key}=", value)
          end
        end
      end

      def run
        raise 'Abstract Method.'
      end

      protected

      def configure_errors_adapter
        raise 'Abstract Method.'
      end

    end

  end
end