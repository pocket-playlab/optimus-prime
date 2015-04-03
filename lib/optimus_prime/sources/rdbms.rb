module OptimusPrime
  module Sources
    class Rdbms < Source
      def initialize(dsn:, query:, **options)
        @db = Sequel.connect(dsn, **options)
        @result = @db[query]
      end

      def each
        @result.each do |row|
          yield row
        end
      end

    end
  end
end
