module OptimusPrime
  module Destinations
    class Rdbms < RdbmsWriter
      def initialize(dsn:, table:, delete_conditions:, **options)
        @delete_conditions = delete_conditions
        super(dsn: dsn, table: table, **options)
        delete_records
      end

      private

      def delete_records
        @table.where(@delete_conditions).delete
      end
    end
  end
end
