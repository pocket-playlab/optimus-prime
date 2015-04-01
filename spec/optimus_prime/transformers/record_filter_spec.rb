require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::RecordFilter do
  let(:constraints) do
    {
      level: { type: :range, values: [0, 100] },
      altitude: { type: :less_than_or_equal, values: [8000] },
      character: { type: :set, values: ['tom', 'jerry'] },
      nil_stuff: { type: :set, values: [nil] },
    }
  end

  let(:filter) do
    OptimusPrime::Transformers::RecordFilter.new(constraints: constraints)
  end

  let(:input_valid) do
    [
      { character: 'tom',   level: 50, altitude: 5612 },
      { character: 'jerry', level: 50, altitude: 4581 },
      { character: 'jerry', level: 10, altitude: 2345 }
    ]
  end

  let(:input_invalid) do
    [
      { character: 'tom',   level: 150, altitude: 5612 },
      { character: 'jerry', level: 50, altitude: 4581 },
      { character: 'jerry', level: 50, altitude: 4582, nil_stuff: nil },
      { character: 'jerry', level: 50, altitude: 8001 },
      { character: 'jerry', level: 2, altitude: 1000, nil_stuff: 'not_nil!' },
      { character: 'james', level: 50, altitude: 4581 }
    ]
  end

  let(:output_invalid) do
    [
      { character: 'jerry', level: 50, altitude: 4582, nil_stuff: nil },
      { character: 'jerry', level: 50, altitude: 4581 }
    ]
  end

  def write_records(destination, input)
    output = []
    destination.output << output
    input.each { |record| destination.write(record) }
    output
  end

  it 'should allow all the records to pass through' do
    output = write_records(filter, input_valid)
    expect(output).to match_array input_valid
  end

  it 'should filter the invalid records' do
    output = write_records(filter, input_invalid)
    expect(output).to match_array output_invalid
  end

  context '#contained, #not_contained' do
    let(:records) do
      [
        { code: 'A-111-3CB' },
        { code: 'B-222-3CB' },
        { code: 'A-333-3CB' },
        { code: 'C-444-3CB' },
        { code: 'C-555-3CB' }
      ]
    end

    let(:start_with_a_or_c) { { code: { type: :not_contained, values: ['B-'] } } }
    let(:start_with_b) { { code: { type: :contained, values: ['B-'] } } }

    let(:records_without_b) do
      [
        { code: 'A-111-3CB' },
        { code: 'A-333-3CB' },
        { code: 'C-444-3CB' },
        { code: 'C-555-3CB' }
      ]
    end

    let(:ac_filter) { OptimusPrime::Transformers::RecordFilter.new(constraints: start_with_a_or_c) }
    let(:b_filter) { OptimusPrime::Transformers::RecordFilter.new(constraints: start_with_b) }

    it 'should not contained b' do
      output = write_records(ac_filter, records)
      expect(output).to match_array records_without_b
    end

    it 'should contained only b' do
      output = write_records(b_filter, records)
      expect(output).to match_array [{ code: 'B-222-3CB' }]
    end
  end
end
