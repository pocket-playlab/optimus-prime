require 'logger'

module OptimusPrime
  # The base class for all data processing steps. Do not subclass `Step`
  # directly, subclass `OptimusPrime::Source` or `OptimusPrime::Destination`
  # instead.
  #
  # Set up a step by adding input and ouput queues like this:
  #
  #     step.input  << inqueue
  #     step.output << outqueue
  #
  # A step must have at least one input or output queue. Once these have been
  # configured, start the step by calling `#start`. The step will then listen
  # on the input queues, passing each value received to the `#process` method.
  #
  # The step will finish processing data when it receives a falsy value (`nil`
  # or `false`).
  class Step
    class << self
      # Keep track of all subclasses so we can retrieve them by name.
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

    include Wisper::Publisher
    attr_accessor :logger, :module_loader

    def start
      raise 'Already started' if started?
      raise 'No input or output' if input.empty? and output.empty?
      input.freeze
      output.freeze
      listen unless input.empty?
      self
    end

    def join
      raise 'Not yet started' unless started?
      threads.each(&:join)
      self
    end
    alias_method :wait, :join

    # Call the `finish` callback and push `nil` to the output queues to signal
    # the end of the data stream.
    def close
      @closed = true
      finish
      push nil
      self
    end

    def started?
      not threads.empty?
    end

    # Has this step reached the end of its input?
    def closed?
      @closed || false
    end

    # Has this step finished processing all its data?
    def finished?
      started? and threads.none?(&:status)
    end

    # The set of input queues. Anything that responds to `#pop` can be added
    # to this set.
    def input
      @input ||= Set.new
    end

    # The set of output queues. Anything that responds to `#push` can be added
    # to this set.
    def output
      @output ||= Set.new
    end

    def hash_with_indifferent_access(message)
      message.is_a?(Hash) ? HashWithIndifferentAccess.new(message) : message
    end

    private

    # Spawn a listener thread for each input queue, then call `#close` when
    # they have all finished.
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
