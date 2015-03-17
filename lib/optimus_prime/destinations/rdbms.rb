module OptimusPrime
  module Destinations
    class Rdbms < RdbmsWriter
      def initialize(dsn:, table:, conditions:, **options)
        @conditions = conditions
        super(dsn: dsn, table: table, **options)
        delete_records
      end

      private

      def delete_records
        @table.where(@conditions).delete
      end
    end
  end
end
