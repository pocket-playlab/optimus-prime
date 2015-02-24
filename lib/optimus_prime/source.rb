module OptimusPrime
  class Source < Step
    include Enumerable

    def each
      raise 'Not implemented'
    end

    def pipe(*)
      super
      producer = Thread.new do
        begin
          each { |record| @output << record }
        ensure
          @finished = true
        end
      end
      producer.abort_on_exception = true
      @output
    end

    def finished?
      @finished || false
    end

  end
end
