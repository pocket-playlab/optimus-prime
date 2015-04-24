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
      def initialize(dsn:, table:, retry_interval: 5, max_retries: 3, **options)
        @db = Sequel.connect(dsn, **options)
        @table = @db[table.to_sym]
        @retry_interval = retry_interval
        @max_retries = max_retries
      end

      def write(record)
        execute do
          begin
            @table.insert record
          rescue Sequel::UniqueConstraintViolation => e
            logger.warn e.to_s
          end
        end
      end

      def finish
        @db.disconnect
      end

      # Receives a block and execute it in a safe context
      # If any exception is raised, it will try again for 'max_retries' times
      # after waiting for 'retry_interval' seconds.
      #
      # execute do
      #   @table.insert record
      # end
      def execute(&block)
        run_block(block)
      rescue Sequel::DatabaseConnectionError => e
        @max_retries -= 1

        if @max_retries > 0
          log_and_sleep(e)
          retry
        else
          raise
        end
      end

      def run_block(block)
        set_instance_variables
        block.call
      end

      def log_and_sleep(error)
        logger.error "Error while connecting to database: #{error}. Sleeping #{@retry_interval}s..."
        sleep @retry_interval
      end

      def set_instance_variables
        @max_retries ||= 3
        @retry_interval ||= 5
      end
    end
  end
end
