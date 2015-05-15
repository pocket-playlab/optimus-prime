module OptimusPrime
  module Transformers
    # Accepts a Hash object and expands the Hash values of specified fields.
    class ExpandHash < ExpandJSON

      private

      def read_value(record, field)
        record[field]
      end

      def error_message(err, record, field)
        "#{err.message}: Cannot expand invalid Hash field '#{field}' in: #{record}"
      end

    end
  end
end
