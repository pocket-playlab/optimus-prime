module OptimusPrime
  module Modules
    module Persistence
      class Listener
        attr_reader :options

        def initialize(dsn:)
          @db = Sequel.connect(dsn)
          @pipeline_name = nil
          @operation_id = nil
          @jobs = {}
        end

        def operation
          @operation ||= Operation.new(@db)
        end

        def load_job
          @load_job ||= LoadJob.new(@db)
        end

        def pipeline_started(pipeline)
          @pipeline_name = pipeline.name
          @operation_id = operation.create pipeline_id: pipeline.name,
                                           start_time: Time.now,
                                           status: :started
        end

        def pipeline_finished(pipeline)
          operation.update id: @operation_id,
                           end_time: Time.now,
                           status: 'finished'
        end

        def pipeline_failed(pipeline, error)
          operation.update id: @operation_id,
                           end_time: Time.now,
                           status: 'failed',
                           error: error
        end

        def load_job_started(job)
          id = "#{Date.today}/#{job.id}"
          @jobs[id] = load_job.create identifier: id,
                                     job_id: job.job_id,
                                     operation_id: @operation_id,
                                     uris: job.uris.join(','),
                                     category: job.id,
                                     status: 'started',
                                     start_time: Time.now
        end

        def load_job_finished(job)
          load_job.update id: @jobs["#{Date.today}/#{job.id}"],
                          status: 'finished',
                          end_time: Time.now
        end

        def load_job_failed(job, error)
          load_job.update id: @jobs["#{Date.today}/#{job.id}"],
                          status: 'failed',
                          end_time: Time.now
        end

      end
    end
  end
end
