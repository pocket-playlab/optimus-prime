module OptimusPrime
  class Pipeline

    attr_reader :graph

    def initialize(graph)
      @graph = graph
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

    def edges
      @edges ||= begin
        visited = Set.new
        sources.keys.flat_map do |key|
          walk(key, visited).map { |from, to| [steps[from], steps[to]]  }
        end
      end
    end

    private

    def walk(from, visited)
      (graph.fetch(from)[:next] || []).lazy.map(&:to_sym).flat_map do |to|
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
