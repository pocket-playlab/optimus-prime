require 'logger'

module OptimusPrime
  # A processing pipeline representing a flow of data from one or more sources
  # to one or more destinations, with any number of intermediate steps.
  #
  # Sources should be subclasses of `OptimusPrime::Source`; destinations and
  # intermediate steps should be subclasses of `OptimusPrime::Destination`.
  #
  # Steps are connected to each other with queues. Start the pipeline by
  # calling `#start`. This will start all steps, which will listen on their
  # input queues and push data to their output queues.
  #
  # Once started, a pipeline will run in the background. To wait for it to
  # finish, call `#wait`.
  class Pipeline
    attr_reader :graph, :logger

    # TODO: configurable queue size
    QUEUE_SIZE = 100

    # Expects a hash representation of the pipeline graph. Keys should be the
    # name of each step, values should be of the form
    #
    #     {
    #       class: 'StepClassName',
    #       params: { step constructor params },
    #       next: ['name of next step', ...]
    #     }
    #
    def initialize(**graph)
      @logger = Logger.new(STDERR)
      @graph = graph
      edges.each do |from, to|
        queue = SizedQueue.new QUEUE_SIZE
        from.output << queue
        to.input    << queue
      end
    end

    def start
      raise 'Already started' if started?
      steps.values.each(&:start)
    end

    def started?
      steps.values.any?(&:started?)
    end

    def finished?
      steps.values.all?(&:finished?)
    end

    def join
      steps.values.each(&:join)
    end
    alias_method :wait, :join

    def steps
      @steps ||= graph.map { |key, config| [key, Step.create(config)] }
                 .each     { |key, step| step.logger = @logger }
                 .to_h
    end

    def edges
      @edges ||= begin
        visited = Set.new
        sources.flat_map do |key|
          walk(key, visited).map { |from, to| [steps[from], steps[to]]  }
        end
      end
    end

    private

    def sources
      graph.keys - graph.values
        .flat_map { |step| step[:next] }
        .compact
        .map(&:to_sym)
    end

    def walk(from, visited)
      (graph.fetch(from)[:next] || []).map(&:to_sym).flat_map do |to|
        visited.include?(to) ? [[from, to]]
                             : [[from, to]] + walk(to, visited.add(to))
      end
    end
  end
end
