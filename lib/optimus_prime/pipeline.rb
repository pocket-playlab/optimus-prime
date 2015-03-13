module OptimusPrime
  class Pipeline
    attr_reader :graph, :logger

    # TODO: configurable queue size
    QUEUE_SIZE = 100

    def initialize(**graph)
      @graph = graph
      edges.each do |from, to|
        queue = SizedQueue.new QUEUE_SIZE
        from.output << queue
        to.input    << queue
      end
      @logger = Logger.new(STDERR)
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
      @steps ||= graph.map { |key, step| [key, Step.create(step)] }.to_h
                      .each do |key, step|
                        step.logger=@logger
                      end
      @steps
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
