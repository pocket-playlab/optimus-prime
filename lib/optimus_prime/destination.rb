module OptimusPrime
  class Destination < Step

    def write(record)
      raise 'Not implemented'
    end

    private

    def process(record)
      raise 'Closed' if closed?
      write record
    end

  end
end
