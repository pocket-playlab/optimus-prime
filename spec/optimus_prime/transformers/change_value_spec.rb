require 'spec_helper'
require 'date'
require 'optimus_prime/transformers/change_value'

RSpec.describe OptimusPrime::Transformers::ChangeValue do
  let(:value_map) do
    {
      '(N/A)' => nil,
      0 => false,
      'test' => 555,
      Date.today => :date
    }
  end

  let(:input) do
    {
      something: 'maps',
      int: 0,
      date: Date.today,
      arr: [],
      str: 'test',
      nil: '(N/A)'
    }
  end

  let(:output) do
    {
      something: 'maps',
      int: false,
      date: :date,
      arr: [],
      str: 555,
      nil: nil
    }
  end

  context 'map with different data types' do
    it 'changes everything correctly' do
      caster = OptimusPrime::Transformers::ChangeValue.new(value_map: value_map)
      result = []
      caster.output << result
      caster.write(input)
      expect(result.first).to eq output
    end
  end
end
