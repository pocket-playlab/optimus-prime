require 'spec_helper'
require 'optimus_prime/transformers/rename_key'

RSpec.describe OptimusPrime::Transformers::RenameKey do
  let(:mapper) { { 'aeg' => 'age', 'weitgh' => 'weight' } }
  let(:step) do
    OptimusPrime::Transformers::RenameKey.new(mapper: mapper)
  end

  context 'input does not contain the replacement name' do
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

    it 'should replace the field name and keep the value' do
      expect(step.run_with(input)).to match_array output
    end
  end

  context 'input contains the replacement name' do
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

    it 'should delete the field and keep the correct field\'s value' do
      expect(step.run_with(input)).to match_array output
    end
  end
end
