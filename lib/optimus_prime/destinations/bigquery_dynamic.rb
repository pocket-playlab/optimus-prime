require 'google/api_client'

module OptimusPrime
  module Destinations
    class BigqueryDynamic < OptimusPrime::Destination
      # This class can push data to multiple BigQuery tables in the same dataset.
      # The way it detects which table a record should go to is with the table_id
      # parameter explained below.
      # { 'prefix' => 'generic_preifx',
      #   'suffix' => 'generic_suffix',
      #   'fields' => ['record', 'fields','to','be','used']
      # }
      # The class also creates tables and updates their schemas if necessary based
      # on the fields of records that it receives.
      # When a new field should be added to the schema, the type of the field is
      # determined using the type_map parameter, which is expected to be a hash.

      attr_reader :client_email, :private_key, :chunk_size

      def initialize(client_email:, private_key:, resource_template:, table_id:,
                     type_map:, id_field: nil, chunk_size: 100)
        @client_email = client_email
        @private_key  = OpenSSL::PKey::RSA.new(private_key)
        @template     = resource_template # https://cloud.google.com/bigquery/docs/reference/v2/tables
        @table_id, @type_map   = table_id, type_map
        @id_field, @chunk_size = id_field, chunk_size
        @tables, @total = {}, 0
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
        table_fields_of(record).join('_')
        .prepend(prefix).concat(suffix)
        .downcase.gsub('.','_')
      end

      def table_fields_of(record)
        record.collect do |field, value|
          value.to_s if @table_id['fields'].include? field.to_s
        end.compact
      end

      def prefix
        @table_id['prefix'] ? "#{@table_id['prefix']}_" : ''
      end

      def suffix
        @table_id['suffix'] ? "_#{@table_id['suffix']}" : ''
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
        # This class deals with a single table in BigQuery.

        attr_reader :id

        def initialize(id, resource_template, type_map, client, id_field = nil)
          @id = id
          @resource = Marshal.load(Marshal.dump(resource_template)) # deep copy for the nested hash
          @resource['tableReference']['tableId'] = id
          @schema = @resource['schema']['fields']
          @client = client
          @type_map, @id_field = type_map, id_field
          @buffer, @exists, @schema_synced = [], nil, false
        end

        def <<(record)
          build_schema(record)
          @buffer << format(record)
        end

        def upload
          return if @buffer.empty?
          create_or_update_table
          insert_all
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
                'type' => @type_map[field]
              }
            end
          return if fields.empty?
          @schema.concat(fields)
          @schema_synced = false
        end

        def insert_all
          response = execute bigquery.tabledata.insert_all,
                             params: { 'tableId' => id },
                             body:   { 'kind' => 'bigquery#tableDataInsertAllRequest',
                                       'rows' => @buffer }
          body = JSON.parse response.body
          failed = body.fetch('insertErrors', []).map { |err| err['index'] }.uniq.length
          raise "Failed to insert #{failed} row(s) to table #{id}" if failed > 0
        end

        def create_or_update_table
          create_table unless table_exists?
          update_table unless @schema_synced
        end

        def update_table
          response = execute bigquery.tables.patch, params: { 'tableId' => id }, body: @resource
          @schema_synced = true
        end

        def create_table
          response = execute bigquery.tables.insert, body: @resource
          @exists = @schema_synced = true
        end

        def table_exists?
          return @exists unless @exists.nil?
          response = execute bigquery.tables.get, params: { 'tableId' => id }
          case response.status
          when 404 then return @exists = false
          when 200 then return @exists = true
          end
        end

        def bigquery
          @bigquery ||= @client.discovered_api 'bigquery', 'v2'
        end

        def execute(method, params: {}, body: nil)
          response = @client.execute(
            api_method:  method,
            parameters:  params.merge('projectId' => project_id,
                                      'datasetId' => dataset_id),
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
      end
    end
  end
end
