module OptimusPrime
  module Modules
    class ModuleLoader
      MODULES = [:persistence, :exceptional]
      attr_reader :persistence, :exceptional, :subscribers

      def initialize(pipeline, modules)
        @pipeline = pipeline
        @subscribers = []
        @modules = modules
        register_modules
      end

      private

      def register_modules
        MODULES.each do |mod|
          send("register_#{mod}") unless @modules[mod]
        end
      end

      def register_persistence
        raise 'Pipeline name required for persistence' unless @pipeline.name
        @persistence = Persistence.new(@modules[:persistence][:options])
        @subscribers << @persistence
      end

      def register_exceptional
        adapter_name = "OptimusPrime::Modules::Exceptional::Adapters::#{@modules[:exceptional][:adapter].capitalize}Adapter"
        return unless Object.const_defined?(adapter_name)
        @exceptional = adapter_name.constantize.new(@modules[:exceptional][:options])
      end
    end
  end
end
