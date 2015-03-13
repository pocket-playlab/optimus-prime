require 'logger'

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
        subclasses do |subclass|
          return subclass if subclass.name == name
        end
        raise "Not found: #{name}"
      end

      def create(**config)
        name = config.fetch :class
        type = find name
        params = config[:params]
        params ? type.new(**params) : type.new
      end

      protected

      def subclasses
        descendants.each do |subclass|
          subclass.subclasses { |s| yield s }
          yield subclass
        end
      end
    end

    attr_reader :logger

    def logger=(logger)
      @logger = logger
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
    alias_method :wait, :join

    def close
      @closed = true
      finish
      push nil
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
        consume queue
      end
      close_after consumers
    end

    def consume(queue)
      background do
        loop do
          message = queue.pop
          break unless message
          process message
        end
      end
    end

    def finish
      # Override this in subclasses if needed
    end

    def close_after(threads)
      background do
        threads.each(&:join)
        close
      end
    end

    def push(message)
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
