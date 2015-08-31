require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::CsvParser do
  let(:step) { OptimusPrime::Transformers::CsvParser.new }

  let(:output) do
    [
      { 'id' => '1', 'name' => 'Alice' },
      { 'id' => '2', 'name' => 'Bob' }
    ]
  end

  context 'when input have header' do
    let(:input) { ["id,name\n1,Alice\n2,Bob"] }

    it 'parse csv into record' do
      expect(step.run_with(input)).to match_array output
    end
  end

  context 'when input not have header' do
    let(:step) { OptimusPrime::Transformers::CsvParser.new(header: false, columns: ['id', 'name']) }
    let(:input) { ["1,Alice\n2,Bob"] }

    it 'buffers everything' do
      expect(step.run_with(input)).to match_array output
    end
  end

  context 'when input is not have header and columns' do
    let(:step) { OptimusPrime::Transformers::CsvParser.new(header: false, columns: []) }
    let(:input) { ["1,Alice\n2,Bob"] }
    it 'raise an error' do
      expect { step.run_with(input) }.to raise_error
    end
  end
end
