require 'thor'
require 'erb'

module OptimusPrime
  class Operator < Thor
    desc 'pipeline <file> <name>', 'Runs the pipeline named <name> inside the config file <file>.'
    method_option :dependencies,
                  aliases: '-d',
                  type: :string,
                  desc: '[Optional] dependencies for this pipeline.'
    def pipeline(file, name)
      OptimusPrime::CLI::PipelineOperator.new(file, name, options[:dependencies]).operate
    end

    desc 'factory <file>', 'Runs all pipelines defined in factory file <file>, sequentially.'
    def factory(file)
      OptimusPrime::CLI::FactoryOperator.new(file).operate
    end
  end

  class OptimusCLI < Thor
    desc 'operate [SUBCOMMAND] ...ARGS', 'run pipeline or factory.'
    map 'o' => :operate
    subcommand 'operate', Operator

    # TODO: implement a generator for pipeline and factory config files!

    desc 'version', 'Display installed OptimusPrime version.'
    map '-v' => :version
    def version
      puts "OptimusPrime #{OptimusPrime::VERSION}"
    end
  end
end
