require 'json'

module OptimusPrime
  module Destinations
    class LocalJson < Destination
      attr_reader :file_path

      def initialize(file_path:)
        @file_path = file_path
      end

      def write(record)
        open_file unless @json
        write_row record
      end

      def finish
        @json.close
      end

      private

      def open_file
        mode = File.exist?(file_path) ? 'a' : 'w'
        @json = File.open(file_path, mode)
      end

      def write_row(record)
        @json.puts JSON.dump record
      end
    end
  end
end
