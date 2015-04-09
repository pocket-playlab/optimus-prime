# The RdbmsDynamic source creates a new row for each given record.
# Expects records to be formatted as Hashes.
#
# This source won't update or delete any rows.

module OptimusPrime
  module Destinations
    class RdbmsWriter < Destination
      # dsn   - Connection string for the database
      # table - Name of the table to use
      # options - all additional parameters are passed to Sequel
      def initialize(dsn:, table:, **options)
        @db = Sequel.connect(dsn, **options)
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

      def finish
        @db.disconnect
      end
    end
  end
end
