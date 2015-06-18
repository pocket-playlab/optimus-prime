require 'logger'
require 'wisper'

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
    include Wisper::Publisher
    
    attr_reader :name, :graph, :logger, :module_loader
    
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
    def initialize(graph, name = nil, modules = {})
      @name = name
      @logger = Logger.new(STDERR)
      @graph = graph
      @module_loader = Modules::ModuleLoader.new(self, modules)
      
      subscribe_all
      edges.each do |from, to|
        queue = SizedQueue.new QUEUE_SIZE
        from.output << queue
        to.input    << queue
      end
    end
    
    def operate
      @module_loader.exception ? @module_loader.exception.run(&method(:run)) : run
    end
    
    def run
      start.join
    end
    
    def start
      raise 'Already started' if started?
      broadcast(:pipeline_started, self)
      steps.values.each(&:start)
      # Returning self allows method chaining (e.g. pipeline.start.join)
      self
    end
    
    def started?
      steps.values.any?(&:started?)
    end
    
    def finished?
      steps.values.all?(&:finished?)
    end
    
    def join
      steps.values.each(&:join)
      broadcast(:pipeline_finished, self)
      # Returning self allows method chaining (e.g. pipeline.join.finished?)
      self
    end
    alias_method :wait, :join
    
    def steps
      @steps ||= graph.map { |key, config| [key, Step.create(config)] }
      .each     { |key, step| step.logger = @logger }
      .each     { |key, step| subscribe_all(step) }
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
    
    def subscribe_all(object = self)
      @module_loader.subscribers.each do |subscriber|
        object.subscribe(subscriber)
      end
    end
    
  end
end
