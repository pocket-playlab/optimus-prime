require_relative 'common/bigquery_table_base'

module OptimusPrime
  module Destinations
    class BigqueryDynamic < OptimusPrime::Destination
      # This class can push data to multiple BigQuery tables in the same dataset.
      # The way it detects which table a record should go to is with the table_id
      # parameter explained below.
      # { 'prefix' => 'generic_prefix',
      #   'suffix' => 'generic_suffix',
      #   'fields' => ['record', 'fields','to','be','used']
      # }
      # The class also creates tables and updates their schemas if necessary based
      # on the fields of records that it receives.
      # When a new field should be added to the schema, the type of the field is
      # determined using the type_map parameter, which is expected to be a hash.
      # Notice that chunck_size should NOT exceed 500 because of BigQuery limits.
      # Please refer to this page for more about streaming limitations:
      # https://cloud.google.com/bigquery/streaming-data-into-bigquery

      attr_reader :client_email, :private_key, :chunk_size, :table_id

      def initialize(client_email:, private_key:, resource_template:, table_id:,
                     type_map:, id_field: nil, chunk_size: 100)
        @client_email = client_email
        @private_key  = OpenSSL::PKey::RSA.new(private_key)
        @template     = resource_template # https://cloud.google.com/bigquery/docs/reference/v2/tables
        self.table_id = table_id
        @type_map     = type_map
        @id_field, @chunk_size = id_field, chunk_size
        @tables, @total = {}, 0
      end

      def table_id=(tid)
        @table_id = stringify_nested_symbolic_keys(tid)
      end

      def stringify_nested_symbolic_keys(h)
        if h.is_a? Hash
          Hash[h.map { |k, v| [k.is_a?(Symbol) ? k.to_s : k, stringify_nested_symbolic_keys(v)] }]
        else
          h
        end
      end

      def write(record)
        tid = determine_table_of(record)
        unless @tables.key? tid
          @tables[tid] = BigQueryTable.new(tid, @template, @type_map, client, @id_field)
          @tables[tid].logger = logger
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
          .downcase.gsub('.', '_')
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
        attr_reader :id, :resource, :client, :id_field
        attr_accessor :logger

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
          @buffer << format(record)
        end

        def upload
          return if buffer.empty?
          rebuild_schema
          create_or_patch_table
          insert_all
          buffer.clear
        end

        private

        include BigQueryTableBase

        def format(record)
          row = { 'json' => record }
          id = @id_field && record[@id_field]
          row['insertId'] = id if id
          row
        end

        def fields
          resource['schema']['fields'].map { |f| f['name'] }.to_set
        end

        def rebuild_schema
          buffer.each do |rec|
            record = rec['json']
            schema_fields = fields
            record_fields = record.reject { |field, value| schema_fields.include? field }
                            .map { |field, value| { 'name' => field, 'type' => @type_map[field] } }
            next if record_fields.empty?
            @schema.concat(record_fields)
            @schema_synced = false
          end
        end

        def create_or_patch_table
          create_table unless exists?
          patch_table  unless @schema_synced
        end
      end
    end
  end
end
