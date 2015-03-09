require 'google/api_client'

module OptimusPrime
  module Destinations
    class BigqueryDynamic < OptimusPrime::Destination
      attr_reader :client_email, :private_key, :chunk_size

      def initialize(client_email:, private_key:, resource_template:, table_id:, type_detective:, id_field: nil, chunk_size: 100)
        @client_email = client_email
        @private_key  = OpenSSL::PKey::RSA.new private_key
        @template     = resource_template # https://cloud.google.com/bigquery/docs/reference/v2/tables
        @table_id     = table_id
        @type_detective = type_detective
        @id_field     = id_field    # optional - used for deduplication
        @chunk_size   = chunk_size
        @tables       = {}
        @total        = 0
      end

      def write(record)
        tid = table_id(record)
        unless @tables.key? tid
          @tables[tid] = BigQueryTable.new(tid, @template, @type_detective, client, @id_field)
        end
        @tables[tid] << record
        @total += 1
        upload if @total >= chunk_size
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

      def table_id(record)
        @table_id.is_a?(Proc) ? @table_id.call(record) : @table_id
      end

      def upload
        @tables.each(&:upload)
        @total = 0
      end

      def finish
        upload unless @total.zero?
      end

      class BigQueryTable
        attr_reader :id

        def initialize(id, resource_template, type_detective, client, id_field: nil)
          @id = id
          @resource = resource_template.clone
          @resource['tableReference']['tableId'] = id
          @schema = @resource['schema']['fields']
          @id_field = id_field
          @buffer = []
          @client = client
          @exists = nil
          @schema_synced = false
        end

        def <<(record)
          build_schema(record)
          @buffer << format(record)
        end

        def upload
          return if @buffer.empty?
          create_or_update_table
          response = insert_all
          body = JSON.parse response.body
          raise body['error']['message'] unless response.status == 200
          failed = body.fetch('insertErrors', []).map { |e| e['index'] }.uniq.length
          raise "Failed to insert #{failed} row(s)" if failed > 0
          @buffer.clear
        end

        private

        def format(record)
          row = { 'json' => record }
          id = @id_field && record[@id_field]
          row['insertId'] = id if id
          row
        end

        def build_schema(record)
          fields = record.collect do |k, v|
            {
              'name' => k,
              'type' => type_detective(k)
            }
          end
          existing_schema_keys = @schema.keys
          fields.keys.each do |k|
            unless existing_schema_keys.include? k
              @schema.concat(fields).uniq!
              @schema_synced = false
              break
            end
          end
        end

        def insert_all
          execute bigquery.tabledata.insert_all,
                  params: { 'tableId' => id },
                  body:   { 'kind' => 'bigquery#tableDataInsertAllRequest',
                            'rows' => @buffer }
        end

        def create_or_update_table
          create_table unless table_exists?
          update_table unless @schema_synced
        end

        def update_table
          response = execute bigquery.tables.patch, params: { 'tableId' => id }, body: @schema
          unless response.status == 200
            body = JSOn.parse response.body
            raise body['error']['message']
          end
          @schema_synced = true
          response
        end

        def create_table
          response = execute bigquery.tables.insert, body: @schema
          unless response.status == 200
            body = JSON.parse response.body
            raise body['error']['message']
          end
          @exists = true
          @schema_synced = true
          response
        end

        def table_exists?
          return @exists unless @exists.nil?
          response = execute bigquery.tables.get, params: { 'tableId' => id }
          case response.status
          when 404 then return @exists = false
          when 200 then return @exists = true
          else
            body = JSON.parse response.body
            raise body['error']['message']
          end
        end

        def bigquery
          @bigquery ||= client.discovered_api 'bigquery', 'v2'
        end

        def execute(method, params: {}, body: nil)
          @client.execute api_method:  method,
                          parameters:  params.merge('projectId' => project_id,
                                                    'datasetId' => dataset_id),
                          body_object: body
        end

        def project_id
          @resource['tableReference']['projectId']
        end

        def dataset_id
          @resource['tableReference']['datasetId']
        end
      end
    end
  end
end
