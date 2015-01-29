module OptimusPrime
  class Container
    attr_accessor :conf_file_path

    def initialize
      # intantiate config object
      @config = OptimusPrime::Conf.new(@conf_file_path)

      self.wire_up_classes
    end

    def wire_up_classes
      # recursively iterate from destinations back to sources and...
      # 1. check if that object has been instantiated already?
      # 2. if not, instantiate it and store it somewhere (hash where key is identifier and value is object?)
    end

    def execute
      # execute a pipeline by calling 'get_data' on it, which results in recursive get_data calls down the pipeline.
      # then call 'put_data' to persist results
    end
  end
end
