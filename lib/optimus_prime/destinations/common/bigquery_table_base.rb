require 'google/api_client'

module OptimusPrime
  module Destinations
    module BigQueryTableBase
      # This module assumes the presence of the following methods:
      # - id: the id of the BigQuery Table.
      # - resource: the resource of the BigQuery Table, see this link:
      #     https://cloud.google.com/bigquery/docs/reference/v2/tables
      # - client: the Googla API client to make queries
      # - id_field: used for deduplication

      MAX_ROWS_PER_INSERT = 500
      MAX_ROWS_PER_SECOND = 10_000
      MAX_RETRIES         = 5

      def time_frame
        @time_frame ||= Time.now.to_i
      end

      def last_total
        @last_total ||= 0
      end

      def check_limits
        if time_frame < Time.now.to_i
          @time_frame = Time.now.to_i
          @last_total = 0
        elsif last_total + buffer.size > MAX_ROWS_PER_SECOND
          sleep 1
          @time_frame = Time.now.to_i
          @last_total = 0
        end
      end

      def buffer
        @buffer ||= []
      end

      def create_table
        execute bigquery.tables.insert, body: @resource
        @exists = @schema_synced = true
      end

      def patch_table
        return unless patch_needed?
        execute bigquery.tables.patch, params: { 'tableId' => id }, body: @resource
        @schema_synced = true
      end

      def patch_needed?
        remote_table = JSON.parse fetch_table.body
        resource['schema']['fields'].concat(remote_table['schema']['fields']).uniq!
        resource['schema']['fields'].length > remote_table['schema']['fields'].length
      end

      def exists?
        @exists ||= (fetch_table.status == 200)
      end

      def fetch_table
        execute bigquery.tables.get, params: { 'tableId' => id }
      end

      def insert_all
        check_limits
        retried = false unless retried
        @last_total += buffer.size
        body = JSON.parse perform_insertion.body
        failed = body.fetch('insertErrors', []).map { |err| err['index'] }.uniq.length
        raise "Failed to insert #{failed} row(s) to table #{id}" unless failed.zero?
      rescue => e
        if retried
          raise e unless body
          # TODO: raise exception if the number of invalid records equals the buffer size
          # it needs better test cases
          body.fetch('insertErrors', []).each do |err|
            logger.error "Insertion Error: #{err} | Record: #{buffer[err['index']]}"
          end
          return
        end
        clean_buffer(body) if body
        sleep 2
        retried = true
        retry
      end

      # Removes successful records to prevent duplication
      def clean_buffer(body)
        invalid_rows = body.fetch('insertErrors', []).map { |err| err['index'] }.to_set
        invalid_records = invalid_rows.each_with_object([]) { |err, obj| obj << buffer[err] }
        buffer.clear.concat(invalid_records)
      end

      def perform_insertion
        execute bigquery.tabledata.insert_all,
                params: { 'tableId' => id },
                body:   { 'kind' => 'bigquery#tableDataInsertAllRequest',
                          'rows' => buffer,
                          'skipInvalidRows' => true,
                          'ignoreUnknownValues' => false }
      end

      def bigquery
        @bigquery ||= client.discovered_api 'bigquery', 'v2'
      end

      def execute(method, params: {}, body: nil)
        retries  ||= 0
        duration ||= 1
        response = perform_request(method, params: params, body: body)
        return response if [200, 404].include?(response.status)
        raise_response(response)
      rescue => e
        raise e unless response and (500..599).include?(response.status) and retries < MAX_RETRIES
        sleep duration
        duration *= 2
        retries += 1
        retry
      end

      def raise_response(response)
        raise "HTTP Status: #{response.status} | message: #{JSON.parse(response.body)}"
      rescue JSON::ParserError
        raise "HTTP Status: #{response.status} | message: #{response.body}"
      end

      def perform_request(method, params: {}, body: nil)
        client.execute(
          api_method:  method,
          parameters:  params.merge('projectId' => project_id, 'datasetId' => dataset_id),
          body_object: body
        )
      end

      def project_id
        @resource['tableReference']['projectId']
      end

      def dataset_id
        @resource['tableReference']['datasetId']
      end

      def fields
        @fields ||= resource['schema']['fields'].map { |f| f['name'] }.to_set
      end
    end
  end
end
