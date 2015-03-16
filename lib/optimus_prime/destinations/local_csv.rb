require 'csv'
require 'stringio'

module OptimusPrime
  module Destinations
    class LocalCsv < Destination
      attr_reader :fields, :file_path

      def initialize(fields:, file_path:)
        @fields = fields
        @file_path = file_path
      end

      def write(record)
        open_file unless @csv
        write_header unless @header_written
        write_row record
      end

      def finish
        @csv.close
      end

      private

      def open_file
        mode = File.exist?(file_path) ? 'a' : 'w'
        @header_written = mode == 'a'
        @csv = CSV.open(file_path, mode)
      end

      def write_header
        @csv << fields
        @header_written = true
      end

      def write_row(record)
        @csv << format(record)
      end

      def format(record)
        fields.map { |key| record[key] }
      end
    end
  end
end
