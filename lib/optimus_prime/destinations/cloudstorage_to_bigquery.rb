require_relative 'common/bigquery_table_base'

module OptimusPrime
  module Destinations
    class CloudstorageToBigquery < OptimusPrime::Destination
      SLEEPING_TIME = 10
      # TODO: make the source format configurable
      SOURCE_FORMAT = 'NEWLINE_DELIMITED_JSON'

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
        jobs = tasks.map { |table, uris| LoadJob.new client, logger, @config, table, uris }
        wait_for_jobs(jobs)
      end

      private

      def wait_for_jobs(jobs)
        loop do
          jobs.select!(&:pending?)
          return if jobs.empty?
          sleep SLEEPING_TIME
        end
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

      class LoadJob
        # for BigQueryTableBase
        attr_reader :client, :logger, :id, :project_id, :dataset_id, :resource

        def initialize(client, logger, config, table, uris)
          @client     = client
          @logger     = logger
          @id         = table
          @project_id = config[:project]
          @dataset_id = config[:dataset]
          @schema     = config[:schema]
          @resource   = generate_resource

          # NOTE: Could be optimised to just fetch the table once
          patch_table if exists?

          insert_request = insert_files(uris)
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

        def insert_files(uris)
          execute(bigquery.jobs.insert, body: generate_job_data(uris))
        end

        def generate_job_data(uris)
          {
            configuration: {
              load: {
                sourceUris: uris,
                schema: @schema,
                sourceFormat: SOURCE_FORMAT,
                destinationTable: { projectId: project_id, datasetId: dataset_id, tableId: id }
              }
            }
          }.stringify_nested_symbolic_keys
        end

        class LoadJobError < StandardError
          # This is in the section "Additional Data" in Sentry
          attr_reader :extra

          def initialize(extra = {})
            @message = 'Load job in BigQuery encountered a problem.'
            @extra = extra
          end
        end
      end
    end
  end
end
