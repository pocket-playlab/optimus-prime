module OptimusPrime
  class Transform < Step

    def transform(message)
      send message
    end

    protected

    def process(message)
      raise 'Closed' if closed?
      transform message
    end

  end
end
