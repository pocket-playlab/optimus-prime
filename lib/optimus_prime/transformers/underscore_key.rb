# The UnderscoreKey transformer formats each key of a Hash
# so it only contains letters, numbers and underscores.
#
# No configuration available.

module OptimusPrime
  module Transformers
    class UnderscoreKey < Destination
      # Will be replaces with an underscore
      CHARS_TO_REPLACE = /[-+=.,\/: ]/
      # Everything but letters, numbers and underscore
      CHARS_TO_REMOVE  = /[^a-zA-Z0-9_]/

      def write(data)
        # Format all keys
        push Hash[data.map { |key, val| [format(key), val] }]
      end

      private

      def format(key)
        key
          .to_s
          .downcase
          .gsub(CHARS_TO_REPLACE, '_')
          .gsub(CHARS_TO_REMOVE, '')
      end
    end
  end
end
