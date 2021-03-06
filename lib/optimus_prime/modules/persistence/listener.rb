module OptimusPrime
  module Modules
    module Persistence
      class Listener
        attr_reader :base
        delegate :operation, :load_job, to: :base

        def initialize(base)
          @base = base
          @operation_id = nil
          @jobs = {}
        end

        def pipeline_started(pipeline)
          @operation_id = operation.create pipeline_id: pipeline.name.to_s,
                                           start_time: Time.now,
                                           status: 'started'
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
          @jobs[job.uris.first] = load_job.create identifier: job.uris.first,
                                                  job_id: job.job_id,
                                                  operation_id: @operation_id,
                                                  uris: job.uris.join(','),
                                                  category: job.id.to_s,
                                                  status: 'started',
                                                  start_time: Time.now
        end

        def load_job_finished(job)
          load_job.update id: @jobs[job.uris.first],
                          operation_id: @operation_id,
                          status: 'finished',
                          end_time: Time.now
        end

        def load_job_failed(job, error)
          load_job.update id: @jobs[job.uris.first],
                          operation_id: @operation_id,
                          status: 'failed',
                          end_time: Time.now,
                          error: error
        end
      end
    end
  end
end
