require 'spec_helper'
require 'optimus_prime/transformers/bigquery_nested_fields_string_converter'

module OptimusPrime
  module Sources
    class MyTestSource < Source
      def initialize(records:)
        @records = records
      end

      def each
        @records.each { |record| yield record }
      end
    end
  end

  module Destinations
    class MyTestDestination < Destination
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

describe OptimusPrime::Transformers::BigqueryNestedFieldsStringConverter do
  def init_pipeline(keys:)
    OptimusPrime::Pipeline.new(
      **{
        src: {
          class: 'OptimusPrime::Sources::MyTestSource',
          params: { records: input },
          next: ['trans']
        },
        trans: {
          class: 'OptimusPrime::Transformers::BigqueryNestedFieldsStringConverter',
          params: { keys: keys },
          next: ['dest']
        },
        dest: { class: 'OptimusPrime::Destinations::MyTestDestination' }
      }
    )
  end

  context 'input hash has string keys' do
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

    it 'converts one level nested-fields string to an array of hash' do
      results = init_pipeline(keys: ['field2', 'field3']).start.wait.steps[:dest].written
      expect(results).to match_array output
    end
  end

  context 'input hash has symbol keys' do
    let(:input) do
      [
        {
          field1: 'value',
          field2: "{\"customer_id\":298,\"type\":\"sms\",\"cost\":2}\n",
          field3: "{\"customer_id\":298,\"type\":\"mms\",\"cost\":5}\n"
        },
        {
          field1: 'value'
        }
      ]
    end

    let(:output) do
      [
        {
          field1: 'value',
          field2: [{ 'customer_id' => 298, 'type' => 'sms', 'cost' => 2 }],
          field3: [{ :customer_id => 298, :type => 'mms', :cost => 5 }]
        },
        {
          field1: 'value'
        }
      ]
    end

    it 'converts one level nested-fields string to an array of hash' do
      results = init_pipeline(keys: [:field2, :field3]).start.wait.steps[:dest].written
      expect(results).to match_array output
    end
  end
end
