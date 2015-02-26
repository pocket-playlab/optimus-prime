module OptimusPrime
  class Destination < Step

    def write(message)
      raise 'Not implemented'
    end

    private

    def process(message)
      raise 'Closed' if closed?
      write message
    end

  end
end
