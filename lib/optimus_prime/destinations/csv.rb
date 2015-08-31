require 'csv'
require 'stringio'

module OptimusPrime
  module Destinations
    class Csv < OptimusPrime::Destinations::S3Destination
      attr_reader :fields, :bucket, :key, :chunk_size

      def initialize(fields:, bucket:, key:, chunk_size: 1024 * 1024 * 10, **options)
        super(bucket: bucket, key: key, chunk_size: chunk_size, **options)
        @fields = fields
        @header_written = false
        reset
      end

      def write(record)
        write_header unless @header_written
        write_row record
        upload_chunk if @buffer.bytesize > chunk_size
      end

      def finish
        if @upload
          upload_chunk if @buffer.bytesize > 0
          complete_upload
        else
          upload_buffer
        end
        push({ bucket: @bucket, key: @key })
      end

      private

      def reset
        @buffer = ''
        @csv = CSV.new @buffer
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
