require 'spec_helper'
require 'optimus_prime/transformers/buffer'

RSpec.describe OptimusPrime::Transformers::Buffer do
  def output_of(input)
    output = []
    transformer.output << output
    input.each { |record| transformer.write(record) }
    output
  end

  let(:source_a) do
    {
      id: 1,
      name: 'Alice'
    }
  end

  let(:source_b) do
    {
      id: 2,
      name: 'Bob'
    }
  end

  let(:source_c) do
    {
      id: 3,
      pet: 'cat',
      age: 4
    }
  end

  def output_of(input)
    buffer = OptimusPrime::Transformers::Buffer.new
    output = []
    buffer.output << output
    input.each { |record| buffer.write(record) }
    buffer.finish
    output
  end

  context 'sources are same keys' do
    let(:output_ab) do
      [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]
    end

    it 'keeps everything' do
      result = output_of([source_a, source_b])
      expect(result).to match_array output_ab
    end
  end

  context 'sources are difference keys' do
    let(:output_ac) do
      [
        { id: 1, name: 'Alice' },
        { id: 3, pet: 'cat', age: 4 },
      ]
    end

    it 'keeps everything' do
      result = output_of([source_a, source_c])
      expect(result).to match_array output_ac
    end
  end
end
