require 'csv'
require 'stringio'

class CsvDestination < OptimusPrime::Destination

  def initialize(header:, bucket:, filename:, chunk_size: 1024 * 1024 * 10)
    @s3 = Aws::S3::Client.new
    @header = header
    @bucket = bucket
    @filename = filename
    @chunk_size = chunk_size
    @header_written = false
    reset
  end

  def write(record)
    write_header unless @header_written
    write_row format record
    upload_chunk if @buffer.bytesize > @chunk_size
  end

  def close
    if @upload
      upload_chunk if @buffer.bytesize > 0
      complete_upload
    else
      upload_buffer
    end
  end

  private

  def reset
    @buffer = ''
    @csv = CSV.new @buffer
  end

  def write_header
    write_row @header
    @header_written = true
  end

  def write_row(row)
    @csv << row
  end

  def format(record)
    @header.map { |key| record[key] }
  end

  def upload_chunk
    @upload ||= @s3.create_multipart_upload bucket: @bucket,
                                            key: @filename
    @parts ||= []
    @parts.push @s3.upload_part bucket: @bucket,
                                key: @filename,
                                body: @buffer,
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
    @s3.complete_multipart_upload bucket: @bucket,
                                  key: @filename,
                                  upload_id: @upload.upload_id,
                                  multipart_upload: { parts: parts }
  end

  def upload_buffer
    @s3.put_object bucket: @bucket,
                   key: @filename,
                   body: @buffer
  end

end
