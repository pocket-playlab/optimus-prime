module OptimusPrime
  # A source of data. Subclass this and implement `#each`. The `#each` method
  # should yield values, which will be sent downstream for processing.
  class Source < OptimusPrime::Step
    include Enumerable

    def each
      raise 'Not implemented'
    end

    def start
      super
      stream
    end

    private

    def stream
      background do
        each do |message|
          raise "#{self.class.name} returned null value" unless message
          push message
        end
        close
      end
    end
  end
end
