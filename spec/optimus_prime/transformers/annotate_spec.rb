require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::Annotate do
  let(:extra) { { 'note' => 'HELLO!' } }

  let(:input) do
    [
      { 'name' => 'John', 'age' => 23 },
      { 'name' => 'Jack', 'age' => 24 }
    ]
  end

  let(:output) do
    [
      { 'name' => 'John', 'age' => 23, 'note' => 'HELLO!' },
      { 'name' => 'Jack', 'age' => 24, 'note' => 'HELLO!' }
    ]
  end

  let(:step) { OptimusPrime::Transformers::Annotate.new(extra) }

  it 'annotates each record with the given keys and values' do
    expect(step.run_with(input)).to match_array output
  end
end
