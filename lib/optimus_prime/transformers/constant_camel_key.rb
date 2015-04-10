# The ConstantCamelKey transformer formats each key of a Hash.
# - It only contains letters and numbers.
# - It is CamelCased.
# - It begins Uppercase.
# - Braces including their content get removed.
#
# No configuration available.

module OptimusPrime
  module Transformers
    class ConstantCamelKey < Destination
      MATCH_BRACES = /\(.*?\)/
      MATCH_SPECIAL_CHARS_AND_SPACES = /[^a-zA-Z0-9]/

      def write(data)
        # Format all keys
        push data.map { |key, val| [format(key), val] }.to_h
      end

      private

      def format(key)
        key
          .gsub(MATCH_BRACES, '')
          .gsub(MATCH_SPECIAL_CHARS_AND_SPACES, '_')
          .camelize
      end
    end
  end
end
