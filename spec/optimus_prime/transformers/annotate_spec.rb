require 'spec_helper'
require 'optimus_prime/transformers/annotate'

RSpec.describe OptimusPrime::Transformers::Annotate do
  let(:extra) { { 'note' => 'HELLO!' } }

  let(:input) do
    [
      { 'name' => 'John', 'age' => 23 },
      { 'name' => 'Jack', 'age' => 24 }
    ]
  end

  let(:expected) do
    [
      { 'name' => 'John', 'age' => 23, 'note' => 'HELLO!' },
      { 'name' => 'Jack', 'age' => 24, 'note' => 'HELLO!' }
    ]
  end

  let(:step) do
    OptimusPrime::Transformers::Annotate.new(extra)
  end

  it 'should annotate each record with the given keys and values' do
    output = []
    step.output << output
    input.each { |record| step.write(record) }
    expect(output).to match_array expected
  end
end
