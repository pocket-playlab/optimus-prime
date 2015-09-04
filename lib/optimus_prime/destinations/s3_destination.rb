require 'csv'
require 'stringio'

module OptimusPrime
  module Destinations
    class S3Destination < OptimusPrime::Destination
      attr_reader :bucket, :key, :chunk_size

      def initialize(bucket:, key:, chunk_size: 1024 * 1024 * 10, **options)
        @s3 = Aws::S3::Client.new(**options)
        @bucket = bucket
        @key = key
        @chunk_size = chunk_size
        reset
      end

      def write(record)
        @buffer << record
        upload_chunk if @buffer.join.bytesize > chunk_size
      end

      def finish
        if @upload
          upload_chunk if @buffer.join.bytesize > 0
          complete_upload
        else
          upload_buffer
        end
        push(bucket: @bucket, key: @key)
      end

      private

      def reset
        @buffer = []
      end

      def upload_chunk
        @upload ||= @s3.create_multipart_upload bucket: bucket,
                                                key: key
        @parts ||= []
        @parts.push @s3.upload_part bucket: bucket,
                                    key: key,
                                    body: @buffer.join,
                                    upload_id: @upload.upload_id,
                                    part_number: @parts.length + 1
        reset
      end

      def complete_upload
        parts = @parts.map.with_index do |part, i|
          {
            etag: part.etag.tr('"', ''),
            part_number: i + 1,
          }
        end
        @s3.complete_multipart_upload bucket: bucket,
                                      key: key,
                                      upload_id: @upload.upload_id,
                                      multipart_upload: { parts: parts }
      end

      def upload_buffer
        @s3.put_object bucket: bucket,
                       key: key,
                       body: @buffer.join
      end
    end
  end
end
