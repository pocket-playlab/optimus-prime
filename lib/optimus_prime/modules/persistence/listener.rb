module OptimusPrime
  module Modules
    module Persistence
      class Listener
        attr_reader :options

        def initialize(dsn:)
          @db = Sequel.connect(dsn)
          @operations = {}
        end

        def operation
          Operation.new(@db)
        end

        def pipeline_started(pipeline)
          id = operation.create pipeline_id: pipeline.name,
                                start_time: Time.now,
                                status: :started
          @operations[pipeline.name] = id
        end

        def pipeline_finished(pipeline)
          operation.update id: @operations[pipeline.name],
                           end_time: Time.now,
                           status: 'finished'
        end

        def pipeline_failed(pipeline, error)
          operation.update id: @operations[pipeline.name],
                           end_time: Time.now,
                           status: 'failed',
                           error: error
        end

      end
    end
  end
end
