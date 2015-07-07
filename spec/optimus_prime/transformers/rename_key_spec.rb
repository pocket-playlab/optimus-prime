require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::RenameKey do
  let(:mapper) { { 'aeg' => 'age', 'weitgh' => 'weight' } }
  let(:step) do
    OptimusPrime::Transformers::RenameKey.new(mapper: mapper)
  end

  context 'when input does not contain the replacement name' do
    let(:input) do
      [
        { 'name' => 'John', 'aeg' => 23, 'height' => 192.3, 'weitgh' => 83.5 },
        { 'name' => 'Jack', 'aeg' => 24, 'height' => 183.5, 'weight' => 79.8 }
      ]
    end
    let(:output) do
      [
        { 'name' => 'John', 'age' => 23, 'height' => 192.3, 'weight' => 83.5 },
        { 'name' => 'Jack', 'age' => 24, 'height' => 183.5, 'weight' => 79.8 }
      ]
    end

    it 'replaces the field name and keep the value' do
      expect(step.run_with(input)).to match_array output
    end
  end

  context 'when input contains the replacement name' do
    let(:input) do
      [
        { 'name' => 'John', 'aeg' => 32, 'age' => 35, 'height' => 192.3, 'weitgh' => 83.5 },
        { 'name' => 'Jack', 'aeg' => 22, 'age' => 24, 'height' => 162.3, 'weitgh' => 65.5 },
      ]
    end
    let(:output) do
      [
        { 'name' => 'John', 'age' => 35, 'height' => 192.3, 'weight' => 83.5 },
        { 'name' => 'Jack', 'age' => 24, 'height' => 162.3, 'weight' => 65.5 }
      ]
    end

    it 'deletes the field and keep the correct field\'s value' do
      expect(step.run_with(input)).to match_array output
    end
  end
end
