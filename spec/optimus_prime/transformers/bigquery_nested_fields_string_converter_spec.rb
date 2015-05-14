require 'spec_helper'
require 'optimus_prime/transformers/bigquery_nested_fields_string_converter'

module OptimusPrime
  module Sources
    class MySource < Source
      def initialize(records:)
        @records = records
      end

      def each
        @records.each { |record| yield record }
      end
    end
  end

  module Destinations
    class MyDestination < Destination
      attr_reader :written
      def initialize
        @written = []
      end

      def write(record)
        @written << record
      end
    end
  end
end

describe OptimusPrime::Transformers::BigQueryNestedFieldsStringConverter do
  let(:input) do
    [
      {
        'field1' => 'value',
        'field2' => "{\"customer_id\":298,\"type\":\"sms\",\"cost\":2}\n",
        'field3' => "{\"customer_id\":298,\"type\":\"mms\",\"cost\":5}\n"
      },
      {
        'field1' => 'value',
        'field2' => "{\"customer_id\":555,\"type\":\"sms\",\"cost\":2}\n"
      },
      {
        'field1' => 'value'
      }
    ]
  end

  let(:output) do
    [
      {
        'field1' => 'value',
        'field2' => [{ 'customer_id' => 298, 'type' => 'sms', 'cost' => 2 }],
        'field3' => [{ 'customer_id' => 298, 'type' => 'mms', 'cost' => 5 }]
      },
      {
        'field1' => 'value',
        'field2' => [{ 'customer_id' => 555, 'type' => 'sms', 'cost' => 2 }]
      },
      {
        'field1' =>  'value'
      }
    ]
  end

  let(:pipeline) do
    OptimusPrime::Pipeline.new(
      **{
        src: {
          class: 'OptimusPrime::Sources::MySource',
          params: { records: input },
          next: ['trans']
        },
        trans: {
          class: 'OptimusPrime::Transformers::BigQueryNestedFieldsStringConverter',
          params: { keys: ['field2', 'field3'] },
          next: ['dest']
        },
        dest: { class: 'OptimusPrime::Destinations::MyDestination' }
      }
    )
  end

  it 'converts one level nested-fields string to an array of hash' do
    results = pipeline.start.wait.steps[:dest].written
    expect(results).to match_array output
  end
end
