module OptimusPrime
  class Step
    class << self

      def inherited(subclass)
        descendants << subclass
      end

      def descendants
        @descendants ||= []
      end

      def find(name)
        subclasses.find do |subclass|
          subclass.name.split('::').last == name
        end
      end

      protected

      def subclasses
        Enumerator.new do |enum|
          descendants.each do |subclass|
            subclass.subclasses.each { |s| enum << s }
            enum << subclass
          end
        end
      end

    end
  end
end
