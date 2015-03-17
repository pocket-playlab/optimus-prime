require_relative 'bigquery_table_base'

module OptimusPrime
  module Destinations
    class Bigquery < OptimusPrime::Destination
      # This class can push data to a single table with a fixed well-known
      # resource model (https://cloud.google.com/bigquery/docs/reference/v2/tables).
      # If you need something dynamic, see the BigqueryDestination class.

      attr_reader :client_email, :private_key, :resource, :id_field, :chunk_size

      def initialize(client_email:, private_key:, resource:, id_field: nil, chunk_size: 100)
        @client_email = client_email
        @private_key  = OpenSSL::PKey::RSA.new private_key
        @resource     = resource       # 
        @id_field     = id_field    # optional - used for deduplication
        @chunk_size   = chunk_size
      end

      def write(record)
        buffer << format(record)
        upload if buffer.length >= chunk_size
      end

      private
      include BigQueryTableBase

      def id
        resource['id']
      end

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

      def format(record)
        row = { 'json' => record.select { |k, v| fields.include? k } }
        id = id_field && record[id_field]
        row['insertId'] = id if id
        row
      end

      def upload
        create_table unless exists?
        insert_all
        buffer.clear
      end

      def finish
        upload unless buffer.empty?
      end
    end
  end
end
