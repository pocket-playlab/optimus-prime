module OptimusPrime
  module Sources
    class Rdbms < Source
      def initialize(dsn:, query:, **options)
        @db = Sequel.connect(dsn, **options)
        @query = query
      end

      def each
        query.each do |row|
          yield row
        end
      end

      private

      def query
        @db[@query]
      end
    end
  end
end
