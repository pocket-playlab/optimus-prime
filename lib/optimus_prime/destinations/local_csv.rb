require 'csv'
require 'stringio'

module OptimusPrime
  module Destinations
    class LocalCsv < Destination
      attr_reader :fields, :append_mode, :file_path

      def initialize(fields:, file_path:, append_mode: false, should_write_header: true, **options)
        @fields = fields
        @file_path = file_path
        @append_mode = append_mode
        @options = options
        @should_write_header = should_write_header

        # always assume that we should not write header if the file already exists and we are appending it
        if @append_mode && File.exist?(@file_path)
          @should_write_header = false
        end

        reset
      end

      def write(record)
        write_header if @should_write_header
        write_row format record
      end

      def close
        @csv.close()
      end

      private

      def reset
        mode = @append_mode ? 'ab' : 'wb'
        @csv = CSV.open(@file_path, mode, @options)
      end

      def write_header
        write_row fields
        @should_write_header = false
      end

      def write_row(row)
        @csv << row
      end

      def format(record)
        fields.map { |key| record[key] }
      end

    end
  end
end
