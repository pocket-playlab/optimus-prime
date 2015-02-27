require 'sequel'

module OptimusPrime
  module Destinations
    class Postgresql < Destination

      def initialize(database_url:, table:, **options)
        db = Sequel.connect(database_url, **options)
        @table = db[table]
      end

      def write(record)
        @table.insert record
      end

    end
  end
end