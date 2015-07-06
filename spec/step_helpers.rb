module OptimusPrime
  class Step
    def prepare_output(*inputs)
      inputs.each { |i| input << i }
      output = []
      self.output << output
      output
    end

    def run_with(*inputs)
      output = prepare_output(*inputs)
      start
      join
      return output.compact
    ensure
      close
    end

    def log_to(logger)
      self.logger = logger
      self
    end

    def log_to_stdout
      with_logger(STDOUT)
    end

    class << self
      def create_with_logger(logger, **config)
        create(**config).log_to(logger)
      end

      def create_with_stdout(**config)
        create(**config).log_to_stdout
      end
    end
  end
end

module StepHelpers
end

RSpec.configure do |config|
  config.include StepHelpers
end
