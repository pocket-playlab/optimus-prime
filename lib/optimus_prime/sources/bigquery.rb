require 'google_bigquery'
require 'json'

module OptimusPrime
  module Sources
    class Bigquery < OptimusPrime::Source
      def initialize(project_id:, sql:, **config_params)
        @project_id = project_id
        @sql = sql
        setup **config_params
        GoogleBigquery::Auth.new.authorize
      end

      def each
        query(@sql).each { |row| yield row }
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
          raise_error "Bigquery#query - #{e}", "@project_id = #{@project_id} | @sql = #{@sql}"
        end
        if result['jobComplete'] && result['pageToken'].nil?
          return map_result_into_rows result['schema']['fields'], result['rows']
        end

        get_query_results result['jobReference']['jobId']
      end

      def get_query_results(job_id)
        rows = []
        request_opt = {}
        loop do
          begin
            result = GoogleBigquery::Jobs.getQueryResults @project_id, job_id, request_opt
          rescue => e
            raise_error "Bigquery#get_query_results - #{e}", "#@project_id = #{@project_id} | job_id = #{job_id} | request_opt = #{request_opt}"
          end
          if result['jobComplete']
            rows.concat(map_result_into_rows(result['schema']['fields'], result['rows'])).uniq!
            request_opt = { pageToken: result['pageToken'] } unless result['pageToken'].nil?
          else
            sleep 3
          end
          break if result['totalRows'].to_i == rows.count
        end
        rows
      end

      # This can be used to map an array of fields and an array of rows
      # into an array of hash. [{ 'field' => 'value' }]
      def map_result_into_rows(fields, rows)
        rows.map do |row|
          Hash[row['f'].map.with_index do |field, index|
            value = if field['v'].nil?
              field['v']
            else
              case fields[index]['type']
              when 'INTEGER'
                field['v'].to_i
              when 'FLOAT'
                field['v'].to_f
              when 'BOOLEAN'
                field['v'] == 'true'
              else
                field['v']
              end
            end
            [fields[index]['name'].to_sym, value]
          end]
        end
      end

      def raise_error(error, params)
        logger.error "#{error} | #{params}"
        raise error
      end
    end
  end
end