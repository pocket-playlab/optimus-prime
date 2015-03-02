require 'sequel'
require 'pry-byebug'

module OptimusPrime
  module Destinations
    class Rdbms < Destination
      def initialize(dsn:, table:, sql_trace: false, **options)
        @db = Sequel.connect(dsn, **options)

        if sql_trace
          @db.loggers << Logger.new($stdout)
          @db.sql_log_level = :debug
        end

        @table = @db[table.to_sym]
      end

      def write(record)
        # log any data that doesn't match what we are expecting
        # not sure if this should be a fatal condition...
        unless record.is_a?(::Hash)
          logger.error 'record was not a Hash as expected!'
          logger.error record.inspect
        end

        @table.insert record
      end

      def close
        @db.disconnect
      end
    end
  end
end
