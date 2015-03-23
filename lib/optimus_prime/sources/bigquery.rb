require 'google_bigquery'
require 'json'

module OptimusPrime
  module Sources
    class Bigquery < OptimusPrime::Source
      def initialize(project_id:, sql:, **config_params)
        @project_id = project_id
        @sql = sql
        setup(**config_params)
        GoogleBigquery::Auth.new.authorize
        @query_response = {}
      end

      def each
        query_results.each { |row| yield row }
      end

      private

      def setup(pass_phrase:, key_file:, email:)
        GoogleBigquery::Config.setup do |config|
          config.pass_phrase = pass_phrase
          config.key_file    = key_file
          config.scope       = 'https://www.googleapis.com/auth/bigquery'
          config.email       = email
          config.retries     = 24
        end
      end

      def query
        sleep_duration = 3
        begin
          logger.info "Querying #{@sql}"
          GoogleBigquery::Jobs.query @project_id, query: @sql
        rescue => e
          logger.error "Bigquery#query - #{e} | ProjectID: #{@project_id} | sql: #{@sql}"
          # BigBroda will raise an error as a string that contains error reason and message.
          if e.message.include? 'rateLimitExceeded'
            sleep sleep_duration
            sleep_duration += 2
            logger.info "Retry querying #{@sql}"
            retry
          end
          raise e
        end
      end

      def query_results
        @query_response = query
        if @query_response['jobComplete'] && !@query_response.key?('pageToken')
          return map_query_response_into_hashes
        end
        query_job @query_response['jobReference']['jobId']
      end

      def get_query_results(job_id, request_opt: {})
        logger.info "Querying jobId: #{job_id}"
        GoogleBigquery::Jobs.getQueryResults @project_id, job_id, request_opt
      rescue => e
        error_params = "ProjectID: #{@project_id} | JobID: #{job_id} | Options: #{request_opt}"
        logger.error "Bigquery#get_query_results - #{e} | #{error_params}"
        raise e
      end

      def query_job(job_id, request_opt: {}, sleep_period: 3)
        Enumerator.new do |enum|
          loop do
            @query_response = get_query_results job_id, request_opt: request_opt
            if process_job(enum, sleep_period)
              break unless @query_response.key? 'pageToken'
              request_opt[:pageToken] = @query_response['pageToken']
            end
          end
        end
      end

      def process_job(enum, sleep_period)
        if @query_response['jobComplete']
          map_query_response_into_hashes.each { |row| enum << row }
          true
        else
          sleep sleep_period
          false
        end
      end

      def response_fields
        @query_response['schema']['fields']
      end

      def response_rows
        @query_response['totalRows'].to_i == 0 ? [] : @query_response['rows']
      end

      # This can be used to map an array of fields and an array of rows
      # into an array of hash. [{ 'field' => 'value' }]
      def map_query_response_into_hashes
        response_rows.map do |row|
          Hash[row['f'].map.with_index do |field, index|
            value = if field['v']
                      field['v'].convert_to response_fields[index]['type']
                    else
                      field['v']
                    end
            [response_fields[index]['name'].to_sym, value]
          end]
        end
      end
    end
  end
end
