require 'json'
require 'stringio'
require 'zlib'
require 'aws-sdk'

module OptimusPrime
  module Sources
    class Csv < OptimusPrime::Sources::S3Source
      def initialize(bucket:, from:, to:, col_sep: ',', **options)
        super(bucket: bucket, from: from, to: to, **options)
        @col_sep = col_sep
      end

      def each
        objects.each do |object|
          csv = CSV.new(object.body, headers: :first_row, col_sep: @col_sep)
          csv.each do |line|
            yield line.to_hash
          end
        end
      end
    end
  end
end
