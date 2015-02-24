module OptimusPrime
  class Source < Step

    def each
      raise 'Not implemented'
    end

    def pipe(*)
      super
      producer = Thread.new do
        each { |record| @output << record }
      end
      producer.abort_on_exception = true
      @output
    end

  end
end
