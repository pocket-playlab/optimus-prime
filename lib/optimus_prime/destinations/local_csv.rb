require 'csv'
require 'stringio'

module OptimusPrime
  module Destinations
    class LocalCsv < Destination
      attr_reader :fields, :append_mode, :file_path

      def initialize(fields:, file_path:, append_mode: false, **options)
        @fields = fields
        @file_path = file_path
        @append_mode = append_mode
        @header_written = false
        @options = options
        reset
      end

      def write(record)
        write_header unless @header_written
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
        @header_written = true
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
