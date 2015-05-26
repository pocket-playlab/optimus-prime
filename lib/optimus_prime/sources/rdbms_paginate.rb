module OptimusPrime
  module Sources
    class RdbmsPaginate < Rdbms
      def initialize(dsn:, query:, rows_per_fetch:, order_field: :id, **options)
        super(dsn: dsn, query: query, **options)
        @rows_per_fetch = rows_per_fetch
        @order_field = order_field
      end

      def each
        @db[@query].order(@order_field).paged_each(rows_per_fetch: @rows_per_fetch) do |row|
          yield row
        end
      end
    end
  end
end
