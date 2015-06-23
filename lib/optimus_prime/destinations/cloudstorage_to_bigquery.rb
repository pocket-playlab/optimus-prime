require_relative 'common/bigquery_table_base'

SLEEPING_TIME = 10
# For now, we just plan to use json
SOURCE_FORMAT = 'NEWLINE_DELIMITED_JSON'

module OptimusPrime
  module Destinations
    class CloudstorageToBigquery < OptimusPrime::Destination

      def initialize(client_email:, private_key:, project:, dataset:, schema:)
        @client_email = client_email
        @private_key  = OpenSSL::PKey::RSA.new(private_key)
        @config = {
          project: project,
          dataset: dataset,
          schema: schema
        }
      end

      def write(tasks)
        jobs = tasks.map do |table, uris|
          if discard_job?(uris.first)
            nil
          else
            job = LoadJob.new client, logger, @config, table, uris
            broadcast(:load_job_started, job)
            job
          end
        end.compact
        wait_for_jobs(jobs)
      end

      private

      def discard_job?(uri)
        return unless module_loader.try(:persistence)

        job = module_loader.persistence.load_job.get(uri)
        return unless job

        if job[:status] == 'failed'
          logger.error("Existing load job found with status 'failed'. Re-running job #{job[:identifier]}")
          false
        else
          logger.error("Existing load job found with status '#{job[:status]}'. Discarding job #{job[:identifier]}")
          true
        end
      end

      def wait_for_jobs(jobs)
        while true
          jobs = jobs.select(&method(:check_status))
          return if jobs.empty?
          sleep SLEEPING_TIME
        end
      end

      def check_status(job)
        pending = job.pending?
        broadcast(:load_job_finished, job) unless pending
        pending
      rescue LoadJobError => e
        broadcast(:load_job_failed, job, e)
        logger.error("Load job in BigQuery encountered a problem: #{e}.")
        false
      end

      def client
        @client ||= begin
          client = Google::APIClient.new application_name:    'Optimus Prime',
                                         application_version: OptimusPrime::VERSION,
                                         auto_refresh_token:  true
          scope = 'https://www.googleapis.com/auth/bigquery'
          asserter = Google::APIClient::JWTAsserter.new @client_email, scope, @private_key
          client.authorization = asserter.authorize
          client
        end
      end

      class LoadJobError < StandardError
        # This is in the section "Additional Data" in Sentry
        attr_reader :extra

        def initialize(extra = {})
          @message = 'Load job in BigQuery encountered a problem.'
          @extra = extra
        end
      end

      class LoadJob
        # for BigQueryTableBase
        attr_reader :client, :logger, :id, :project_id, :dataset_id, :resource, :job_id, :uris

        def initialize(client, logger, config, table, uris)
          @client     = client
          @logger     = logger
          @id         = table
          @project_id = config[:project]
          @dataset_id = config[:dataset]
          @schema     = config[:schema]
          @resource   = generate_resource
          @uris = uris

          # NOTE: Could be optimised to just fetch the table once
          patch_table if exists?

          insert_request = insert_files
          @job_id = JSON.parse(insert_request.body)['jobReference']['jobId']
          logger.info "LoadJob created #{@job_id} (table: #{id})."
        end

        def pending?
          request = execute(bigquery.jobs.get, params: { 'jobId' => @job_id })
          body = JSON.parse(request.body)
          error = body['status']['errorResult']

          raise LoadJobError.new(body) if error
          state = body['status']['state'].downcase
          logger.info "LoadJob #{@job_id} (table: #{id}) is #{state}."
          state != 'done'
        end

        private

        include BigQueryTableBase

        def generate_resource
          { schema: @schema }.stringify_nested_symbolic_keys
        end

        def insert_files
          execute(bigquery.jobs.insert, body: generate_job_data)
        end

        def generate_job_data
          {
            configuration: {
              load: {
                sourceUris: @uris,
                schema: @schema,
                sourceFormat: SOURCE_FORMAT,
                destinationTable: {
                  projectId: project_id,
                  datasetId: dataset_id,
                  tableId: id
                }
              }
            }
          }.stringify_nested_symbolic_keys
        end

      end

    end
  end
end
