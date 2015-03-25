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
        execute bigquery.tables.patch, params: { 'tableId' => id }, body: @resource
        @schema_synced = true
      end

      def exists?
        @exists ||= (fetch_table.status == 200)
      end

      def fetch_table
        execute bigquery.tables.get, params: { 'tableId' => id }
      end

      def insert_all
        check_limits
        @last_total += buffer.size
        response = execute bigquery.tabledata.insert_all,
                           params: { 'tableId' => id },
                           body:   { 'kind' => 'bigquery#tableDataInsertAllRequest',
                                     'rows' => buffer }
        body = JSON.parse response.body
        failed = body.fetch('insertErrors', []).map { |err| err['index'] }.uniq.length
        raise "Failed to insert #{failed} row(s) to table #{id}" if failed > 0
      end

      def bigquery
        @bigquery ||= client.discovered_api 'bigquery', 'v2'
      end

      def execute(method, params: {}, body: nil)
        response = client.execute(
          api_method:  method,
          parameters:  params.merge('projectId' => project_id, 'datasetId' => dataset_id),
          body_object: body
        )
        return response if [200, 404].include? response.status
        raise JSON.parse(response.body)['error']['message']
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
