module OptimusPrime
  class Source < OptimusPrime::Step
    include Enumerable

    def each
      raise 'Not implemented'
    end

    def start
      super
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
