require 'spec_helper'
require 'optimus_prime/transformers/buffer'

RSpec.describe OptimusPrime::Transformers::Buffer do
  let(:buffer) { OptimusPrime::Transformers::Buffer.new }

  context 'when records have the same keys' do
    let(:sample) do
      [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' }
      ]
    end

    it 'buffers everything' do
      expect(buffer.run_with(sample.dup)).to match_array sample
    end
  end

  context 'when records have different keys' do
    let(:sample) do
      [
        { id: 1, name: 'Alice' },
        { id: 3, pet: 'cat', age: 4 }
      ]
    end

    it 'buffers everything' do
      expect(buffer.run_with(sample.dup)).to match_array sample
    end
  end
end
