# The ChangeValue transformer changes values of a Hash
# to different values based on a value_map.
#
# Any type of value is supported.
# Not matching values stay untouched.
#
# Example value_map:
#
# {
#   '(N/A)' => nil,
#   0 => false,
#   'test' => 555,
#   Date.today => :date
# }

module OptimusPrime
  module Transformers
    class ChangeValue < Destination
      # value_map - Hash of existing values to transform to new values
      def initialize(value_map: {})
        @value_map = value_map
      end

      # Change the value for each entry in the given Hash.
      def write(data)
        push Hash[data.map { |key, val| [key, change(val)] }]
      end

      # Returns the replacement value or the old value if no replacement found
      def change(val)
        @value_map.fetch(val, val)
      end
    end
  end
end
