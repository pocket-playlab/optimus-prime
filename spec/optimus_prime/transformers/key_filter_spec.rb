require 'spec_helper'
require 'optimus_prime/transformers/key_filter'

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

  let(:correct_output) do
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

  let(:filter) do
    OptimusPrime::Transformers::KeyFilter.new(fields: fields)
  end

  it 'should only allow specified fields in the output' do
    output = []
    filter.output << output
    input.each { |record| filter.write(record) }
    expect(output).to match_array correct_output
  end
end
