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

  let(:step) { OptimusPrime::Transformers::Annotate.new(extra) }

  it 'should annotate each record with the given keys and values' do
    expect(step.run_with(input)).to match_array expected
  end
end
