module OptimusPrime
  module CLI
    # This is the abstract class that runners must descent from.
    class OptimusOperator
      attr_accessor :config

      def initialize(config_file)
        self.config = YAML.load(ERB.new(File.read(config_file)).result)
      end

      def operate
        raise 'Not Implemented!'
      end
    end
  end
end
