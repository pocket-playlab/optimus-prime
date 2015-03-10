require 'csv'
require 'stringio'

module OptimusPrime
  module Sources
    class LocalCsv < Source
      def initialize(file_path:, col_sep: ',')
        @file_path = file_path
        @col_sep = col_sep
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
        @csv = CSV.new(open(@file_path), headers: :first_row, col_sep: @col_sep)
      end
    end
  end
end
