require 'optimus_prime'
require 'optimus_prime/destination'

module OptimusPrime
  module Destinations
    class RenameKey < Destination
      # This class accepts a hash object and replaces incorrect
      # keys with correct ones based on the mapping hash given
      # in the initializer.

      def initialize(mapper:)
        @fields = mapper.keys # Caching for faster performance
        @mapper = mapper
      end

      def write(record)
        push transform(record)
      end

      private

      def transform(record)
        record.keys.each do |field|
          next unless @fields.include?(field)
          record[@mapper[field]] ||= record[field]
          record.delete field
        end
        record
      end
    end
  end
end