require 'google/api_client'

module OptimusPrime
  module Destinations
    class Bigquery < OptimusPrime::Destination
      attr_reader :client_email, :private_key, :table, :id_field, :chunk_size

      def initialize(client_email:, private_key:, table:, id_field: nil, chunk_size: 100)
        @client_email = client_email
        @private_key  = OpenSSL::PKey::RSA.new private_key
        @table        = table       # https://cloud.google.com/bigquery/docs/reference/v2/tables
        @id_field     = id_field    # optional - used for deduplication
        @chunk_size   = chunk_size
      end

      def write(record)
        create_table unless table_exists?
        insert record
      end

      private

      def client
        @client ||= begin
          client = Google::APIClient.new application_name:    'Optimus Prime',
                                         application_version: OptimusPrime::VERSION,
                                         auto_refresh_token:  true
          scope = 'https://www.googleapis.com/auth/bigquery'
          asserter = Google::APIClient::JWTAsserter.new(client_email, scope, private_key)
          client.authorization = asserter.authorize
          client
        end
      end

      def bigquery
        @bigquery ||= client.discovered_api 'bigquery', 'v2'
      end

      def insert(record)
        buffer << format(record)
        upload if buffer.length >= chunk_size
      end

      def format(record)
        row = { 'json' => record.select { |k, v| fields.include? k } }
        id = id_field && record[id_field]
        row['insertId'] = id if id
        row
      end

      def upload
        insert_all
        buffer.clear
      end

      def insert_all
        response = execute bigquery.tabledata.insert_all,
                           params: { 'tableId' => table['id'] },
                           body:   { 'kind' => 'bigquery#tableDataInsertAllRequest',
                                     'rows' => buffer }
        body = JSON.parse response.body
        failed = body.fetch('insertErrors', []).map { |e| e['index'] }.uniq.length
        raise "Failed to insert #{failed} row(s)" if failed > 0
      end

      def finish
        upload unless buffer.empty?
      end

      def buffer
        @buffer ||= []
      end

      def create_table
        execute bigquery.tables.insert, body: table
        @table_exists = true
      end

      def table_exists?
        @table_exists ||= (fetch_table.status == 200)
      end

      def fetch_table
        execute bigquery.tables.get, params: { 'tableId' => table['id'] }
      end

      def execute(method, params: {}, body: nil)
        response = client.execute(
          api_method:  method,
          parameters:  params.merge('projectId' => project_id, 'datasetId' => dataset_id),
          body_object: body
        )
        unless [200, 404].include? response.status
          raise JSON.parse(response.body)['error']['message']
        end
        response
      end

      def project_id
        table['tableReference']['projectId']
      end

      def dataset_id
        table['tableReference']['datasetId']
      end

      def fields
        @fields ||= table['schema']['fields'].map { |f| f['name'] }.to_set
      end
    end
  end
end
