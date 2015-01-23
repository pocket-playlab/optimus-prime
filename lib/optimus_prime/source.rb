module OptimusPrime
  class Source
    def columns
      raise "The 'columns' method is not defined in subclass! Please define before continuing."
    end

    def retrieve_data
      raise "The 'retrieve_data' method is not defined in subclass! Please define before continuing."
    end
  end
end
