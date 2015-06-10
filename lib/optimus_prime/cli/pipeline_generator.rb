require_relative 'optimus_generator'

module OptimusPrime
  module CLI
    class PipelineGenerator < OptimusGenerator
      SKELETON = 'pipeline'

      def initialize(steps:, trigger:, deps:)
        dependencies = deps.split(',')
        require_dependencies(dependencies)
        step_paths = steps.map { |step| absolute_template_path(destruct(step)) }
        self.variables = OpenStruct.new({
          trigger: trigger,
          steps: step_paths,
          dependencies: dependencies
        })
      end

      private

      def destruct(step)
        splitted = step.split('::')
        namespace = splitted.first(splitted.size - 1)
        namespace = namespace.empty? ? ['OptimusPrime'] : namespace
        file_path = splitted.last.split(':')
        { file_path: file_path, namespace: namespace }
      end

      def require_dependencies(dependencies)
        dependencies.each do |dependency|
          require dependency
          puts "Required dependency '#{dependency}' has been loaded."
        end
        puts 'All dependencies have been successfully loaded.'
      end
    end
  end
end
