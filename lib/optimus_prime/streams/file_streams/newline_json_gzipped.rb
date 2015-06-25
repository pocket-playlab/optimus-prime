require 'json'
require 'zlib'

module OptimusPrime
  module Streams
    module FileStreams
      class NewlineJsonGzipped
        # Stores a JSON stream of objects in files, each object on a line. Each file contains
        # a maximum number of objects equal to the `max_per_file` parameter. Files are eagerly
        # created, which means a file is opened once the stream is created, or once a file is
        # full it's closed and a new empty file is opened immediately.
        # The stream takes the following arguments:
        # - `folder` a folde in which all files will be stored.
        # - `max_per_file` the maximum number of records a single file can contain before its
        #   closed and a new file is created.
        # - `options`: an hash of options. It can include:
        #   * `level`: the GZip compression level. If not specified, it defaults to `9` (best
        #      compression). if `nil` is specified, it will fallback to the Zlib default value
        #      value (see http://ruby-doc.org/stdlib-2.2.2/libdoc/zlib/rdoc/Zlib/Deflate.html#method-c-new).
        #   * `strategy`: the GZip compression strategy. If not specified or `nil` is specified,
        #     it will default to `Zlib::DEFAULT_STRATEGY`.
        #   If additional options are supplied, a `Zlib::StreamError` will be raised.

        attr_accessor :logger, :record_count, :file_count, :current_file, :folder
        attr_reader :folder, :max_per_file

        def initialize(folder, max_per_file, options = {})
          @folder = folder
          @max_per_file = max_per_file
          @record_count = 0
          @file_count = 1
          @zoptions = compression_options(options)
          @current_file = Zlib::GzipWriter.open(File.join(folder, "#{@file_count}.jgz"), *@zoptions)
        end

        def <<(record)
          current_file.puts JSON.dump(record)
          @record_count += 1
          (@record_count < max_per_file) ? nil : recycle
        end

        def close
          complete_file = File.expand_path(current_file.to_io)
          current_file.close
          @record_count < 1 ? nil : complete_file
        end

        private

        def recycle
          complete_file = File.expand_path(current_file.to_io)
          current_file.close
          @record_count = 0
          @file_count += 1
          @current_file = Zlib::GzipWriter.open(File.join(folder, "#{@file_count}.jgz"), *@zoptions)
          complete_file
        end

        def compression_options(options)
          opts = {
            level: Zlib::BEST_COMPRESSION,
            strategy: Zlib::DEFAULT_STRATEGY
          }.merge(options)
          opts.each { |k, v| raise Zlib::StreamError unless [:level, :strategy].include? k }
          [opts[:level], opts[:strategy]]
        end
      end
    end
  end
end
