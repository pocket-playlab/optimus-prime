module OptimusPrime
  class Source
    def columns
      raise "The 'columns' method is not defined in subclass! Please define before continuing."
    end

    def retrieve_data
      raise "The 'retrieve_data' method is not defined in subclass! Please define before continuing."
    end

    # should have method to store errors (array of exceptions? or error messages?)
  end
end
