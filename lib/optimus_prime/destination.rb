module OptimusPrime
  # A step that receives input from other steps. Subclass this and implement
  # this `#write` method. The `#write` method can call `#push` any number of
  # times to send data downstream.
  class Destination < OptimusPrime::Step
    def write(message)
      raise 'Not implemented'
    end

    private

    def process(message)
      raise 'Closed' if closed?
      write hash_with_indifferent_access(message)
    end
  end
end
