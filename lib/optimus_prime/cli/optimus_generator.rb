module OptimusPrime
  module CLI
    class OptimusGenerator
      attr_accessor :variables, :content

      def initialize
        raise 'Not Implemented!'
      end

      private

      def generate
        self.content = OptimusPrime::Template.new(SKELETON).result(variables)
      end

      def save_to(file)
        File.write(file, content)
      end

      def absolute_template_path(file_path:, namespace: ["OptimusPrime"])
        File.join(namespace.reduce(Module,:const_get)::TEMPLATE_PATH, file_path)
      end
    end
  end
end
