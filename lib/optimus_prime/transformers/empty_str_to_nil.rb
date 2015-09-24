# This transformer converts empty strings to nil ruby values.
# This transformer is needed for Postgres Timestamp types since PG will not
# accept an empty '' value.

module OptimusPrime
  module Transformers
    class EmptyStrToNil < Destination
      # fields - List of fields to check empty strings
      def initialize(fields: [])
        @fields = fields
      end

      # Change the value for each entry in the given fields list.
      def write(data)
        push change(data)
      end

      # Returns nil if empty string detected.
      def change(row)
        @fields.each do |field|
          if row[field].is_a?(String)
            row[field] = nil if row[field].to_str.lstrip.rstrip.empty?
          end
        end
        row
      end
    end
  end
end
