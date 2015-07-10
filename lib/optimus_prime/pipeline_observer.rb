require 'logger'
require 'wisper'

module OptimusPrime
  class PipelineObserver
    attr_accessor :logger

    def initialize(logger: nil)
      @logger = logger || Logger.new(STDOUT)
    end

    def step_closed(step, consumed, produced)
      logger.info(
        "#{step.class.name}:#{step.object_id} closed, consumed #{consumed} records, produced = #{produced} records")
    end
  end
end
