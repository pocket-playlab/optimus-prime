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
      def initialize(dsn:, table:, delete_conditions:, **options)
        @delete_conditions = delete_conditions
        @records_deleted = false
        super(dsn: dsn, table: table, **options)
      end

      def write(record)
        delete_records unless @records_deleted
        super
      end

      private

      def delete_records
        @table.where(@delete_conditions).delete
        @records_deleted = true
      end
    end
  end
end
