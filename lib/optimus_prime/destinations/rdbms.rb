module OptimusPrime
  module Destinations
    class Rdbms < RdbmsWriter
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
