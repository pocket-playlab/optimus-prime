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

    BUFFER_SIZE = 100

    def pipe(to)
      raise 'Already piped' if @output
      @output = SizedQueue.new BUFFER_SIZE
      to.listen @output
      @output
    end

    def listen(queue)
      raise 'Already listening' if @listener
      @listener = Thread.new do
        loop { write queue.pop }
      end
      @listener.abort_on_exception = true
    end

  end
end
