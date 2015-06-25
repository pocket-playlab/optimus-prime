require 'pathname'

module OptimusPrime
  module Transformers
    class FileStreamDistributor < Destination
      # This transformer distributes input records to multiple `FileStreams` substreams,
      # based on the records `category`. It produces pairs of category/file as its output.
      #
      # Input: Expected to be a stream of hashes.
      # Output: a stream of pairs of category/file. Multiple files can have the same category.
      #
      # Parameters:
      # - `path_template`: a template from which a path will be derived and assigned to each
      #   substream to store its files in. The template is a string with optionally one or
      #   moreembedded `%{field}`, which will be substituted with the value of the `field`
      #   key from the record.
      # - `base_path`: the base path that all substream pathes will be relative to. This is
      #   useful if you want to store all of your substreams in one path, maybe to clear or
      #   compress it later. Defaults to `/tmp`.
      # - `category_template`: a template that will be applied to each record to specify its
      #   category, and hence the substream it will be sent to. Similar to `path_template`,
      #   it is a string that contains embedded fields from the record.
      # - `stream_type`: the type of all the substreams. It can be any `FileStreams` stream.
      # - `stream_options`: stream-specific options that will be passed down to substreams,
      #   along with the path and the `max_per_file`. Defaults to an empty hash `{}`.
      # - `max_per_file`: the maximum number of records to store in each file of each of the
      #   substreams. Defaults to `1,000,000`.

      attr_reader :category_template, :path_template, :base_path, :max_per_file, :stream_type

      def initialize(path_template:, base_path: '/tmp',
          category_template:, stream_type:, stream_options: {}, max_per_file: 1_000_000)
        @category_template = category_template
        @path_template = path_template
        @base_path = Pathname.new(base_path)
        @max_per_file = max_per_file
        @stream_type = stream_type.is_a?(String) ? constantize(stream_type) : stream_type
        @streams = {}
        @stream_options = stream_options
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
          @streams[category] = stream_type.new(folder.to_path, max_per_file, @stream_options)
          @streams[category].logger = logger
        end
        @streams[category]
      end
    end
  end
end
