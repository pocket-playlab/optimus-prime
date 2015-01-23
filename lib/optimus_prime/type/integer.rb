module OptimusPrime
  class Type::Integer
    def self.valid?(value)
      value.is_a? Integer
    end
  end
end
