module StepHelpers
  def prepare_output(*inputs)
    inputs.each { |i| input << i }
    output = []
    self.output << output
    output
  end

  def run_with(*inputs)
    output = prepare_output(*inputs)
    self.start.join # will be closed automatically if everything is OK
    output.compact
  end

  def run_and_close(*inputs)
    output = prepare_output(*inputs)
    self.start.join
    output.compact
  ensure # closing the step (and call finish) if not closed automatically
    close unless closed?
  end

  def run_and_raise(*inputs)
    output = prepare_output(*inputs)
    self.start.join
    output.compact
  rescue
    threads.each(&:kill)
    raise
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

module OptimusPrime
  class Step
    include StepHelpers
  end
end
