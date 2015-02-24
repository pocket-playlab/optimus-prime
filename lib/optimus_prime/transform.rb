module OptimusPrime
  class Transform < Step

    def transform(record)
      push record
    end

    protected

    def process(record)
      transform record
    end

    private

    def push(transformed)
      @output.each do |queue|
        queue << transformed
      end
    end

  end
end
