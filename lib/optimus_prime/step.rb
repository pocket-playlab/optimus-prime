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
          subclass.display_name == name
        end
      end

      def display_name
        name.split('::').last
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

    def start
      raise 'Already started' if started?
      raise 'No input or output' if input.empty? and output.empty?
      input.freeze
      output.freeze
      listen unless input.empty?
    end

    def join
      raise 'Not yet started' unless started?
      threads.each(&:join)
    end

    def started?
      not threads.empty?
    end

    def closed?
      @closed || false
    end

    def finished?
      started? and threads.none?(&:status)
    end

    def input
      @input ||= Set.new
    end

    def output
      @output ||= Set.new
    end

    private

    def listen
      consumers = input.map do |queue|
        background do
          loop do
            message = queue.pop
            break unless message
            process message
          end
        end
      end
      background do
        consumers.each(&:join)
        close
      end
    end

    def close
      @closed = true
      send nil
    end

    def send(message)
      output.each { |queue| queue << message }
    end

    def threads
      @threads ||= []
    end

    def background
      thread = Thread.new do
        yield
      end
      thread.abort_on_exception = true
      threads << thread
      thread
    end

  end
end
