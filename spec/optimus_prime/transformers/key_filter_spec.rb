require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::KeyFilter do
  let(:input) do
    [
      { 'name' => 'John', 'age' => 25, 'gender' => :male,   'likes'  => 'Baseball' },
      { 'name' => 'Rita', 'age' => 21, 'gender' => :female, 'hates'  => 'dragons'  },
      { 'name' => 'Tony', 'age' => 14, 'gender' => :male,   'height' => 192.8      },
      { 'name' => 'Lura', 'age' => 36, 'gender' => :female, 'skills' => 'chess'    },
      { 'favourite colour' => 'mauve' }
    ]
  end

  let(:output) do
    [
      { 'name' => 'John', 'age' => 25, 'gender' => :male   },
      { 'name' => 'Rita', 'age' => 21, 'gender' => :female },
      { 'name' => 'Tony', 'age' => 14, 'gender' => :male   },
      { 'name' => 'Lura', 'age' => 36, 'gender' => :female }
    ]
  end

  let(:fields) do
    ['name', 'age', 'gender']
  end

  let(:step) do
    OptimusPrime::Transformers::KeyFilter.new(fields: fields)
  end

  it 'allows only specified fields in the output' do
    expect(step.run_with(input)).to match_array output
  end
end
