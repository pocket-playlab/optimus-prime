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

    def pipe(queue)
      raise 'Already started' if running?
      @output ||= Set.new
      @output.add queue
    end

    def listen(queue)
      raise 'Already started' if running?
      @input ||= Set.new
      @input.add queue
    end

    def start
      raise 'Already started' if running?
      raise 'No input or output' unless @input or @output
      @started = true
      return unless @input
      @input.each do |queue|
        consumer = Thread.new do
          loop { process queue.pop }
        end
        consumer.abort_on_exception = true
      end
    end

    def running?
      @started || false
    end

  end
end
