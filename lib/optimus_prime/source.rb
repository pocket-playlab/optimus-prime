module OptimusPrime
  class Source < Step
    include Enumerable

    def each
      raise 'Not implemented'
    end

    def start
      super
      background do
        each do |message|
          raise "#{self.class.display_name} returned null value" unless message
          send message
        end
        close
      end
    end

  end
end
