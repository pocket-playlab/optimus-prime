module OptimusPrime
  class Container
    attr_accessor :conf_file_path, :objects

    def initialize
      # intantiate config object
      @config = OptimusPrime::Conf.new(@conf_file_path)

      @objects = { }

      self.wire_up_classes_for
    end

    def wire_up_classes_for(identifier)

      # recursively iterate from destinations back to sources and...
      # 1. check if that object has been instantiated already?
      unless @objects[identifier]
        # 3. if not, instantiate it and store it somewhere (hash where key is identifier and value is object?)
        # get the conf hash from conf object
        conf = @config.get_by_id(identifier)

        # 2. handle inheritance
        if conf['parent_class'] 

          merged_conf = self.handle_inheritance(conf)


          self.wire_up_classes_for(conf['parent_class'])
        end

        # recursively check that dependencies are instantiated already
        self.wire_up_classes_for(conf['source'])
      end
    end

    # recursively handle inheritance by merging conf hashes where child keys overwrite parents
    def handle_inheritance(conf)
      return conf unless conf['parent_class']

      parent_conf = @config.get_by_id(conf['parent_class'])

      # raise exception if types are not compatible (for now that means equal)
      raise "parent_class is not equal to child class!" unless parent_conf['type'] eq conf['type']
      
      self.handle_inheritance(parent_conf).merge(conf)
    end

    def execute
      # execute a pipeline by calling 'get_data' on it, which results in recursive get_data calls down the pipeline.
      # then call 'put_data' to persist results
    end
  end
end
