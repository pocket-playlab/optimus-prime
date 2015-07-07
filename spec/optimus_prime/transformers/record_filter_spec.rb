require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::RecordFilter do
  let(:constraints) do
    {
      level: { type: :range, values: [0, 100] },
      altitude: { type: :less_than_or_equal, values: [8000] },
      character: { type: :not_set, values: [nil, 'james'] },
      nil_stuff: { type: :set, values: [nil] },
    }
  end

  let(:step) do
    OptimusPrime::Transformers::RecordFilter.new(constraints: constraints)
  end

  let(:input_valid) do
    [
      { 'character' => 'tom',   'level' => 50, 'altitude' => 5612 },
      { 'character' => 'jerry', 'level' => 50, 'altitude' => 4581 },
      { 'character' => 'jerry', 'level' => 10, 'altitude' => 2345 }
    ]
  end

  let(:input_invalid) do
    [
      { 'character' => 'tom',   'level' => 150, 'altitude' => 5612 },
      { 'character' => 'jerry', 'level' => 50, 'altitude' => 4581 },
      { 'character' => 'jerry', 'level' => 50, 'altitude' => 4582, 'nil_stuff' => nil },
      { 'character' => 'jerry', 'level' => 50, 'altitude' => 8001 },
      { 'character' => 'jerry', 'level' => 2, 'altitude' => 1000, 'nil_stuff' => 'not_nil!' },
      { 'character' => 'james', 'level' => 50, 'altitude' => 4581 },
      { 'level' => 12, 'altitude' => 3142 }
    ]
  end

  let(:output_invalid) do
    [
      { 'character' => 'jerry', 'level' => 50, 'altitude' => 4582, 'nil_stuff' => nil },
      { 'character' => 'jerry', 'level' => 50, 'altitude' => 4581 }
    ]
  end

  it 'allows valid records to pass' do
    expect(step.run_with(input_valid.dup)).to match_array input_valid
  end

  it 'filters out invalid records' do
    expect(step.run_with(input_invalid)).to match_array output_invalid
  end

  describe '#contained, #not_contained' do
    let(:records) do
      [
        { 'code' => 'A-111-3CB' },
        { 'code' => 'B-222-3CB' },
        { 'code' => 'A-333-3CB' },
        { 'code' => 'C-444-3CB' },
        { 'code' => 'C-555-3CB' }
      ]
    end

    let(:start_with_a_or_c) { { code: { type: :not_contained, values: ['B-'] } } }
    let(:start_with_b) { { code: { type: :contained, values: ['B-'] } } }

    let(:records_without_b) do
      [
        { 'code' => 'A-111-3CB' },
        { 'code' => 'A-333-3CB' },
        { 'code' => 'C-444-3CB' },
        { 'code' => 'C-555-3CB' }
      ]
    end

    let(:ac_filter) { OptimusPrime::Transformers::RecordFilter.new(constraints: start_with_a_or_c) }
    let(:b_filter) { OptimusPrime::Transformers::RecordFilter.new(constraints: start_with_b) }

    it 'filters b from the output' do
      expect(ac_filter.run_with(records)).to match_array records_without_b
    end

    it 'lists only b record' do
      expect(b_filter.run_with(records)).to match_array [{ 'code' => 'B-222-3CB' }]
    end
  end
end
