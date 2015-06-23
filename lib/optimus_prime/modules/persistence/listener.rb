module OptimusPrime
  module Modules
    module Persistence
      class Listener
        attr_reader :base
        delegate :operation, :load_job, to: :base

        def initialize(base)
          @base = base
          @pipeline_name = nil
          @operation_id = nil
          @jobs = {}
        end

        def pipeline_started(pipeline)
          @pipeline_name = pipeline.name
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
          load_job.create identifier: job.uris.first,
                          job_id: job.job_id,
                          operation_id: @operation_id,
                          uris: job.uris.join(','),
                          category: job.id.to_s,
                          status: 'started',
                          start_time: Time.now
        end

        def load_job_finished(job)
          load_job.update identifier: job.uris.first,
                          status: 'finished',
                          end_time: Time.now
        end

        def load_job_failed(job, error)
          load_job.update identifier: job.uris.first,
                          status: 'failed',
                          end_time: Time.now
        end
      end
    end
  end
end
