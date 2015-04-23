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
      def initialize(dsn:, table:, retry_interval: 5, max_retries: 3, chunk_size: 100,
                     **options)
        @db = Sequel.connect(dsn, **options)
        @table = @db[table.to_sym]
        @retry_interval = retry_interval
        @max_retries = max_retries
        @chunk_size = chunk_size
        @records = []
      end

      def write(record)
        # log any data that doesn't match what we are expecting
        # not sure if this should be a fatal condition...
        unless record.is_a?(::Hash)
          logger.error 'record was not a Hash as expected!'
          logger.error record.inspect
        end

        @records << record
        multi_insert if @records.length == @chunk_size
      end

      def finish
        multi_insert unless @records.empty?
        @db.disconnect
      end

      # Receives a block and execute it in a safe context
      # If any exception is raised, it will try again for 'max_retries' times
      # after waiting for 'retry_interval' seconds.
      #
      # execute do
      #  @table.multi_insert records
      # end
      def execute(&block)
        run_block(block)
      rescue Sequel::DatabaseConnectionError => e
        @max_retries -= 1

        if @max_retries > 0
          log_and_sleep(e)
          retry
        else
          raise "Couldn't execute block: #{e}"
        end
      end

      def run_block(block)
        set_instance_variables
        block.call
      end

      def log_and_sleep(e)
        logger.error "Error while connecting to database: #{e}. Sleeping #{@retry_interval}s..."
        sleep @retry_interval
      end

      def set_instance_variables
        @max_retries ||= 3
        @retry_interval ||= 5
      end

      def multi_insert
        execute { @table.multi_insert(@records) }
        @records.clear
      end
    end
  end
end
