module OptimusPrime
  class Pipeline

    attr_reader :graph, :queues

    BUFFER_SIZE = 100

    def initialize(graph)
      @graph = graph
      @queues = edges.map do |from, to|
        queue = SizedQueue.new BUFFER_SIZE
        from.pipe queue
        to.listen queue
        queue
      end
    end

    def start
      raise 'Already started' if running?
      @started = true
      @monitor = Thread.new do
        # TODO: queues empty but downstream still working
        sleep 1 until sources.values.all?(&:finished?) and queues.all?(&:empty?)
      end
      steps.values.each(&:start)
    end

    def running?
      @started || false
    end

    def join
      @monitor.join
      destinations.values.each(&:close)
    end

    def steps
      @steps ||= @graph.map { |key, step| [key, instantiate(step)] }.to_h
    end

    def sources
      @sources ||= begin
        tail = graph.values
          .flat_map { |step| step[:next] }
          .compact
          .map(&:to_sym)
          .to_set
        steps.reject { |key, step| tail.include? key }
      end
    end

    def destinations
      @destinations ||= graph
        .select { |key, step| not step[:next] or step[:next].empty? }
        .keys
        .map { |key| [key, steps[key]] }
        .to_h
    end

    def transforms
      @transforms ||= begin
        keys = (sources.keys + destinations.keys).to_set
        steps.reject { |key, step| keys.include? key }
      end
    end

    private

    def edges
      @edges ||= begin
        visited = Set.new
        sources.keys.flat_map do |key|
          walk(key, visited).map { |from, to| [steps[from], steps[to]]  }
        end
      end
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
