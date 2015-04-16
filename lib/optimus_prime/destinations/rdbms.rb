# The Rdbms source extends RdbmsWriter.
# It not just inserts new rows but also offers
# the option to delete data from the database before the first insertion.

module OptimusPrime
  module Destinations
    class Rdbms < RdbmsWriter
      # dsn   - Connection string for the database
      # table - Name of the table to use
      # delete_conditions - used in the #where method to find the rows to delete
      # options - all additional parameters are passed to Sequel
      def initialize(dsn:, table:, retry_interval: 5, delete_conditions:, **options)
        @delete_conditions = delete_conditions
        @records_deleted = false
        @retry_interval = retry_interval
        super(dsn: dsn, table: table, **options)
      end

      def write(record)
        run_delete_query unless @records_deleted
        super
      end

      private

      def run_delete_query
        delete_records
      rescue Sequel::DatabaseConnectionError => e
        logger.error "Error while connecting to database: #{e}. Sleeping #{@retry_interval}s..."
        sleep @retry_interval
        retry
      end

      def delete_records
        @table.where(@delete_conditions).delete
        @records_deleted = true
      end
    end
  end
end
