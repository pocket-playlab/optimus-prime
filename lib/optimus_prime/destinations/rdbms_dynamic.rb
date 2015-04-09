# The RdbmsDynamic source creates a new row for each given record
# and automatically adds columns to the table as needed.
#
# Expects records to be formatted as Hashes.
#
# For each field the key is used as column name,
# so make sure it is in the correct format.
# The type for the column is determined from the type of the value.
# Only use values with types supported by Sequel.
#
# This source won't update or delete any rows.
#
# For params have a look at RdbmsWriter#initialize.

module OptimusPrime
  module Destinations
    class RdbmsDynamic < RdbmsWriter
      def write(record)
        super(record) if ensure_columns(record)
      end

      private

      # Create missing columns.
      # Return a boolean indicating if operation was successful.
      def ensure_columns(record)
        columns = missing_columns(record)
        add_columns(columns) unless columns.empty?
        true
      rescue Sequel::Error => e
        logger.error "Exception handled - #{e.class}: #{e.message} - record: #{record}"
        false
      end

      # Find columns missing in database.
      # Return a Hash of column names and their types
      #
      # Example:
      # {
      #   name: String,
      #   sample: Integer
      # }
      def missing_columns(record)
        table_columns = @table.columns
        Hash[
          record
            .reject { |key| table_columns.include? key }
            .map { |key, val| [key, val.class] }
        ]
      end

      # Adds a Hash of columns to the DB.
      # Resets @table with new schema
      def add_columns(columns)
        table_name = @table.opts[:from].first

        @db.alter_table table_name do
          columns.each do |name, type|
            add_column(name, type)
          end
        end

        # Reset table to have new schema
        @table = @db[table_name]
      end
    end
  end
end
