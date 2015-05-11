require_relative 'optimus_operator'

module OptimusPrime
  module CLI
    # This class wraps and runs a pipeline
    class PipelineOperator < OptimusOperator
      attr_accessor :name, :pipeline
      attr_reader :cli_dependencies

      def initialize(config_file, name, cli_dependencies = '')
        super(config_file)
        if config.key? name
          self.name = name
        else
          raise "Pipeline #{name} does not exist in #{config_file}"
        end
        @cli_dependencies = cli_dependencies ? cli_dependencies.split(',') : []
        require_dependencies
        load_errors_adapter
        self.pipeline = OptimusPrime::Pipeline.new graph
      end

      def operate
        @errors_adapter ? @errors_adapter.run(&method(:start_pipeline)) : start_pipeline
      end

      def finished?
        pipeline.finished?
      end

      private

      def start_pipeline
        pipeline.start.wait.finished? ? puts('Pipeline finished.') : raise('Pipeline failed to finish!')
      end

      def graph
        @graph ||= config[name]['graph'].symbolize_nested_keys
      end

      def load_errors_adapter
        errors_config = config[name]['errors']
        return unless errors_config
        adapter_name = "OptimusPrime::Adapters::#{errors_config['adapter'].capitalize}Adapter"
        @errors_adapter = Object.const_defined?(adapter_name) ? adapter_name.constantize.new(errors_config['options']) : nil
      end

      def require_dependencies
        yaml_dependencies = config[name]['dependencies'] || []
        (cli_dependencies + yaml_dependencies).uniq.each do |dependency|
          puts "Requiring #{dependency}"
          require(dependency)
        end
      end
    end
  end
end
