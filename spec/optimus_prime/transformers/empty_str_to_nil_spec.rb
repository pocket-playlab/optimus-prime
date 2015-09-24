require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::EmptyStrToNil do
  let(:fields) do
    [
      'field1',
      'field3',
      'field4'
    ]
  end

  let(:input) do
    [
      {
        'field1' => '',
        'field2' => 'blah',
        'field3' => '  ',
        'field4' => nil,
      },
      {
        'field1' => ' my string  ',
        'field2' => nil,
        'field3' => 1,
        'field4' => 'test',
      }
    ]
  end

  let(:output) do
    [
      {
        'field1' => nil,
        'field2' => 'blah',
        'field3' => nil,
        'field4' => nil,
      },
      {
        'field1' => ' my string  ',
        'field2' => nil,
        'field3' => 1,
        'field4' => 'test',
      }
    ]
  end

  context 'map with different data' do
    it 'changes empty strings to nil for fields specified' do
      step = OptimusPrime::Transformers::EmptyStrToNil.new(fields: fields)
      expect(step.run_with(input)).to match_array output
    end
  end
end
