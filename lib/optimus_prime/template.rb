require 'erb'

module OptimusPrime
  class Template
    attr_accessor :template

    def initialize(skeleton)
      self.template = ERB.new(file_content(skeleton))
    end

    def render(variables = nil)
      puts result(variables)
    end

    def result(variables = nil)
      template.result(variables.instance_eval { binding })
    end

    private

    def skeleton_content(file)
      File.read(File.join(OptimusPrime::TEMPLATE_PATH, file))
    end
  end
end
