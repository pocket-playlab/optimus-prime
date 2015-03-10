require 'csv'
require 'stringio'

module OptimusPrime
  module Sources
    class LocalCsv < Source
      def initialize(file_path:)
        @file_path = file_path
      end

      def each
        open_file unless @csv
        @csv.each do |line|
          yield line.to_hash
        end
      end

      def finish
        @csv.close
      end

      private

      def open_file
        raise 'File Not Existing' unless File.exist?(@file_path)
        @csv = CSV.new(open(@file_path), headers: :first_row)
      end
    end
  end
end
