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
      output.add queue
    end

    def listen(queue)
      raise 'Already started' if running?
      input.add queue
    end

    def start
      raise 'Already started' if running?
      raise 'No input or output' if input.empty? and output.empty?
      @started = true
      input.each do |queue|
        consumer = Thread.new do
          loop { process queue.pop }
        end
        consumer.abort_on_exception = true
      end
    end

    def running?
      @started || false
    end

    def finished?
      input.all?(&:empty?)
    end

    private

    def input
      @input ||= Set.new
    end

    def output
      @output ||= Set.new
    end

  end
end
