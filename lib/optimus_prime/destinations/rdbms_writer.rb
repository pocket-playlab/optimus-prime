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
      #   @table.multi_insert record
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

      # [Note for Dataset#multi_insert from Sequel documentation]
      # Be aware that all hashes should have the same keys if you use this calling method,
      # otherwise some columns could be missed or set to null instead of to default values.
      def multi_insert
        add_missing_fields!
        execute do
          begin
            @table.multi_insert(@records)
          rescue Sequel::UniqueConstraintViolation => e
            logger.warn e.to_s
          end
        end
        @records.clear
      end

      def add_missing_fields!
        fields = @records.map(&:keys).flatten.uniq
        @records.each do |record|
          (fields - record.keys).each do |missing_field|
            record.merge!(missing_field => nil)
          end
        end
      end
    end
  end
end
