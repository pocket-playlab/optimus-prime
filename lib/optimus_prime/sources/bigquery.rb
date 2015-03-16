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
      end

      def each
        query.each { |row| yield row }
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
        begin
          result = GoogleBigquery::Jobs.query @project_id, query: @sql
        rescue => e
          logger.error "Bigquery#query - #{e} | ProjectID: #{@project_id} | sql: #{@sql}"
          raise error
        end
        if result['jobComplete'] && result['pageToken'].nil?
          return map_result_into_hashes result['schema']['fields'], result['rows']
        end

        query_job result['jobReference']['jobId']
      end

      def get_query_results(job_id, request_opt: {})
        GoogleBigquery::Jobs.getQueryResults @project_id, job_id, request_opt
      rescue => e
        error_params = "ProjectID: #{@project_id} | JobID: #{job_id} | Options: #{request_opt}"
        logger.error "Bigquery#get_query_results - #{e} | #{error_params}"
        raise e
      end

      def query_job(job_id, request_opt: {}, sleep_period: 3)
        Enumerator.new do |enum|
          loop do
            result = get_query_results job_id, request_opt: request_opt
            if process_job(enum, result, sleep_period)
              break if result['pageToken'].nil?
              request_opt[:pageToken] = result['pageToken']
            end
          end
        end
      end

      def process_job(enum, result, sleep_period)
        if result['jobComplete']
          map_result_into_hashes(result['schema']['fields'], result['rows']).each do |row|
            enum << row
          end
          true
        else
          sleep sleep_period
          false
        end
      end

      def convert_value(field_type, value)
        case field_type
        when 'INTEGER'
          value.to_i
        when 'FLOAT'
          value.to_f
        when 'BOOLEAN'
          value == 'true'
        else
          value
        end
      end

      # This can be used to map an array of fields and an array of rows
      # into an array of hash. [{ 'field' => 'value' }]
      def map_result_into_hashes(fields, rows)
        rows.map do |row|
          Hash[row['f'].map.with_index do |field, index|
            value = if field['v'].nil?
                      field['v']
                    else
                      convert_value fields[index]['type'], field['v']
                    end
            [fields[index]['name'].to_sym, value]
          end]
        end
      end
    end
  end
end
