require 'json'

module OptimusPrime
  module Transformers
    # This class accepts a hash object and converts
    # one level nested-fields string to an array of hash
    # for preparing data before inserting into Google BigQuery.
    #
    # Example:
    # - record:
    #  {
    #    'field1' => 'value',
    #    'field2' => "{\"id\":298,\"type\":\"sms\",\"cost\":2}\n",
    #    'field3' => "{\"id\":299,\"type\":\"mms\",\"cost\":5}\n"
    #  }
    # - keys: ["field2", "field3"]
    #
    # - output:
    #  {
    #    'field1' => 'value',
    #    'field2' => [{ 'id' => 298, 'type' => 'sms', 'cost' => 2 }],
    #    'field3' => [{ 'id' => 299, 'type' => 'mms', 'cost' => 5 }]
    #  }
    class BigQueryNestedFieldsStringConverter < Destination
      def initialize(keys:)
        @keys = keys
      end

      def write(record)
        push convert(record)
      end

      private

      def convert(record)
        @keys.each do |key|
          record[key] = [JSON.parse(record[key])] if record.key?(key)
        end
        record
      end
    end
  end
end
