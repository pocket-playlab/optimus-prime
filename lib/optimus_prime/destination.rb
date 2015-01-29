module OptimusPrime
  class Destination 
    def columns
      raise "The 'columns' method is not defined in subclass! Please define before continuing."
    end

    def put_data
      raise "The 'put_data' method is not defined in subclass! Please define before continuing."
    end
  end
end
