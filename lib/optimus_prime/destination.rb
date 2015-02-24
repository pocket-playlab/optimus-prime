module OptimusPrime
  class Destination < Step

    def write(record)
      raise 'Not implemented'
    end

    def close
    end

    protected

    def process(record)
      write record
    end

  end
end
