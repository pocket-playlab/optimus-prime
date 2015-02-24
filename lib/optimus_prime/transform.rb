module OptimusPrime
  class Transform < Step

    def write(record)
      push record
    end

    private

    def push(transformed)
      @output << transformed
    end

  end
end
