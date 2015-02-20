module OptimusPrime
  module Step
    def inherited(subclass)
      descendants << subclass
    end

    def descendants
      @descendants ||= []
    end

    def find(name)
      descendants.find { |c| c.name.split('::').last == name } or raise "Not found: #{name}"
    end
  end
end
