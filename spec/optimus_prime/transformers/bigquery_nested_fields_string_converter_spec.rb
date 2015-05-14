require 'spec_helper'
require 'optimus_prime/transformers/bigquery_nested_fields_string_converter'

module OptimusPrime
  module Sources
    class MySource < Source
      def initialize(events:)
        @events = events
      end

      def each
        @events.each { |event| yield event }
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
  let(:inputs) do
    [
      {
        "field1" => "value",
        "field2" => "{\"customer_id\":298,\"type\":\"sms\",\"cost\":2}\n",
        "field3" => "{\"customer_id\":298,\"type\":\"mms\",\"cost\":5}\n"
      },
      {
        "field1" => "value",
        "field2" => "{\"customer_id\":555,\"type\":\"sms\",\"cost\":2}\n"
      },
      {
        "field1" => "value"
      }
    ]
  end

  let(:output) do
    [
      {
        "field1" => "value",
        "field2" => [{ "customer_id"=>298, "type"=>"sms", "cost"=>2 }],
        "field3" => [{ "customer_id"=>298, "type"=>"mms", "cost"=>5 }]
      },
      {
        "field1" => "value",
        "field2" => [{ "customer_id"=>555, "type"=>"sms", "cost"=>2 }]
      },
      {
        "field1" =>  "value"
      }
    ]
  end

  let(:sources) do
    Hash[inputs.map.with_index do |input, idx|
      ["src_#{idx}".to_sym, {
        class: 'OptimusPrime::Sources::MySource',
        params: { events: input },
        next: ['trans_c']
      }]
    end]
  end

  let(:pipeline) do
    OptimusPrime::Pipeline.new(**{
      trans_c: {
        class: 'OptimusPrime::Transformers::BigQueryNestedFieldsStringConverter',
        params: { join_keys: [:Platform, :Level] },
        next: ['dest_d']
      },
      dest_d: { class: 'OptimusPrime::Destinations::MyDestination' }
    }.merge(sources))
  end

  it 'converts one level nested-fields string to an array of hash' do
    results = pipeline.start.wait.steps[:dest_d].written
    expect(results).to match_array output
  end
end
