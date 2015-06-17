require 'pathname'

module OptimusPrime
  module Transformers
    class FileStreamDistributor < Destination
      attr_reader :category_template, :path_template, :base_path, :max_per_file, :stream_type

      def initialize(path_template:, base_path: '/tmp',
          category_template:, stream_type:, max_per_file: 1_000_000)
        @category_template = category_template
        @path_template = path_template
        @base_path = Pathname.new(base_path)
        @max_per_file = max_per_file
        @stream_type = stream_type.is_a?(String) ? constantize(stream_type) : stream_type
        @streams = {}
      end

      def write(original_record)
        record = original_record.symbolize_keys
        category = category_of(record)
        stream = find_or_create_stream(category, record)
        file_path = stream << original_record
        push_pair(category, file_path)
      end

      def finish
        @streams.each do |category, stream|
          file_path = stream.close
          push_pair(category, file_path) if file_path
        end
      end

      private

      def constantize(name)
        name.split('::').reduce(Module, :const_get)
      end

      def push_pair(category, file_path)
        push(
          category: category,
          file: Pathname.new(file_path).relative_path_from(base_path).to_path
        ) if file_path
      end

      def path_for(record)
        path_template % record
      end

      def category_of(record)
        (category_template % record).downcase.gsub('.', '_')
      end

      def find_or_create_stream(category, record)
        unless @streams.key? category
          folder = Pathname.new(File.join(base_path, path_for(record)))
          folder.mkpath
          @streams[category] = stream_type.new(folder.to_path, max_per_file)
          @streams[category].logger = logger
        end
        @streams[category]
      end
    end
  end
end
