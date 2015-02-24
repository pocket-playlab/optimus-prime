module OptimusPrime
  class Pipeline

    attr_reader :graph, :queues

    BUFFER_SIZE = 100

    def initialize(graph)
      @graph = graph
      @queues = edges.map do |from, to|
        queue = SizedQueue.new BUFFER_SIZE
        from.output << queue
        to.input    << queue
        queue
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

    def steps
      @steps ||= @graph.map { |key, step| [key, instantiate(step)] }.to_h
    end

    private

    def edges
      @edges ||= begin
        visited = Set.new
        sources.flat_map do |key|
          walk(key, visited).map { |from, to| [steps[from], steps[to]]  }
        end
      end
    end

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

    def instantiate(step)
      name = step.fetch :class
      type = Step.find name
      raise "Not found: #{name}" unless type
      step[:params] ? type.new(**step[:params])
                    : type.new
    end

  end
end
