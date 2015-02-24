module OptimusPrime
  class Source < Step
    include Enumerable

    def each
      raise 'Not implemented'
    end

    def start
      super
      producer = Thread.new do
        begin
          each do |record|
            output.each { |queue| queue << record }
          end
        ensure
          @finished = true
        end
      end
      producer.abort_on_exception = true
    end

    def finished?
      @finished || false
    end

  end
end
