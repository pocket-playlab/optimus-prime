require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::Buffer do
  let(:step) { OptimusPrime::Transformers::Buffer.new }

  context 'when records have the same keys' do
    let(:input) do
      [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' }
      ]
    end

    it 'buffers everything' do
      expect(step.run_with(input.dup)).to match_array input
    end
  end

  context 'when records have different keys' do
    let(:input) do
      [
        { id: 1, name: 'Alice' },
        { id: 3, pet: 'cat', age: 4 }
      ]
    end

    it 'buffers everything' do
      expect(step.run_with(input.dup)).to match_array input
    end
  end
end
