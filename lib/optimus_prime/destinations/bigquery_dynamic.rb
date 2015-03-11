require 'google/api_client'

module OptimusPrime
  module Destinations
    class BigqueryDynamic < OptimusPrime::Destination
      attr_reader :client_email, :private_key, :chunk_size

      # Legends:
      # table_id should be something like this (optionally omit any key/value pair):
      # { 'prefix' => 'generic_preifx',
      #   'suffix' => 'generic_suffix',
      #   'fields' => ['fields','to','be','used']
      # }
      #
      # type_map should be a hash of key/value pairs
      #
      def initialize(client_email:, private_key:,
                     resource_template:, table_id:, type_map:,
                     id_field: nil, chunk_size: 100)
        @client_email = client_email
        @private_key  = OpenSSL::PKey::RSA.new private_key
        @template     = resource_template # https://cloud.google.com/bigquery/docs/reference/v2/tables
        @table_id     = table_id
        @type_map     = type_map
        @id_field     = id_field    # optional - used for deduplication
        @chunk_size   = chunk_size
        @tables       = {}
        @total        = 0
      end

      def write(record)
        tid = determine_table_of(record)
        unless @tables.key? tid
          @tables[tid] = BigQueryTable.new(tid, @template, @type_map, client, @id_field)
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

      def determine_table_of(record)
        res = (@table_id['prefix'].nil? ? "" : @table_id['prefix'] + '_')
        res += record.collect do |field, value|
          value if @table_id['fields'].include? field.to_s
        end.compact.join('_').to_s
        res += ('_' + @table_id['suffix']) unless @table_id['suffix'].nil?
        res.downcase.gsub('.', '_')
      end

      def upload
        @tables.each { |name, obj| obj.upload }
        @total = 0
      end

      def finish
        upload unless @total.zero?
        @tables.clear
      end

      class BigQueryTable
        attr_reader :id

        def initialize(id, resource_template, type_map, client, id_field = nil)
          @id = id
          @resource = Marshal.load(Marshal.dump(resource_template)) # deep copy for the nested hash
          @resource['tableReference']['tableId'] = id
          @schema = @resource['schema']['fields']
          @id_field = id_field
          @type_map = type_map
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
          raise "Failed to insert #{failed} row(s) to table #{id}" if failed > 0
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
          existing_fields = @schema.collect { |column| column['name'] }
          fields = record.reject { |field, value| existing_fields.include? field }
                         .collect do |field, value|
                            { 'name' => field,
                              'type' => determine_type_of(field) }
                          end
          unless fields.empty?
            @schema.concat(fields)
            @schema_synced = false
          end
        end

        def determine_type_of(field)
          @type_map[field]
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
          response = execute bigquery.tables.patch, params: { 'tableId' => id }, body: @resource
          unless response.status == 200
            body = JSON.parse response.body
            raise body['error']['message']
          end
          @schema_synced = true
          response
        end

        def create_table
          response = execute bigquery.tables.insert, body: @resource
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
          @bigquery ||= @client.discovered_api 'bigquery', 'v2'
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
