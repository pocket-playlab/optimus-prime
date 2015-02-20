module OptimusPrime
  class Pipeline

    attr_reader :graph

    def initialize(graph)
      @graph = graph
    end

    private

    def sources
      @sources ||= begin
        tail = graph.values.flat_map { |step| step['next'] }.compact.to_set
        graph
          .reject { |key, step| tail.include? key }
          .map    { |key, step| [key, Source.find(step.fetch('class')).new(step.fetch('params'))] }
          .to_h
      end
    end

    def destinations
      @destinations ||= graph
        .select { |key, step| not step['next'] or step['next'].empty? }
        .map    { |key, step| [key, Destination.find(step.fetch('class')).new(step.fetch('params'))] }
        .to_h
    end

    def transforms
      @transforms ||= graph
        .select { |key, step| not (sources.keys + destinations.keys).include? key }
        .map    { |key, step| [key, Transform.find(step.fetch('class')).new(step.fetch('params'))] }
        .to_h
    end

    def steps
      @steps ||= sources.merge(transforms).merge(destinations)
    end

    def edges
      @edges ||= begin
        visited = Set.new
        sources.keys.flat_map do |key|
          walk(key, visited).map { |from, to| [steps[from], steps[to]]  }
        end
      end
    end

    def walk(from, visited)
      (graph.fetch(from)['next'] || []).flat_map do |to|
        visited.include?(to) ? [[from, to]]
                             : [[from, to]] + walk(to, visited.add(to))
      end
    end

  end
end
