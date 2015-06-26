require 'json'
require 'zlib'

module OptimusPrime
  module Streams
    module FileStreams
      class NewlineJsonGzipped
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
