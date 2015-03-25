require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::RecordFilter do
  let(:constraints) do
    {
      level: { type: :range, values: [0, 100] },
      altitude: { type: :less_than_or_equal, values: [8000] },
      character: { type: :set, values: ['tom', 'jerry'] },
      nil_stuff: { type: :set, values: [nil] }
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

end
