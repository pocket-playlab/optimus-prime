require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::Validator do
  let(:constraints) do
    {
      'level'     => { type: 'range',                 values: [0, 100]           },
      'score'     => { type: 'greater_than_or_equal', values: [0]                },
      'altitude'  => { type: 'less_than_or_equal',    values: [8888]             },
      'character' => { type: 'set',                   values: ['tom',   'jerry'] },
      'lorem'     => { type: 'set',                   values: ['ipsum', 'dolor'] }
    }
  end

  let(:logfile) { '/tmp/validator.log' }

  let(:step) do
    OptimusPrime::Transformers::Validator.new(constraints: constraints).log_to(Logger.new(logfile))
  end

  before { File.delete(logfile) }

  context 'with legal input' do
    let(:input) do
      [
        { 'character' => 'tom',   'level' => 92, 'score' => 220.5, 'altitude' => 5612 },
        { 'character' => 'jerry', 'level' => 73, 'score' => 211.2, 'altitude' => 4581 },
        { 'character' => 'jerry', 'level' => 22, 'score' => 111.2, 'altitude' => 2345 }
      ]
    end

    it 'passes away all records' do
      expect(step.run_with(input.dup)).to match_array input
    end
  end

  context 'with illegal input' do
    let(:input) do
      [
        { 'character' => 'tom',   'level' => 92, 'score' => 220.5, 'altitude' => 5612 },
        { 'character' => 'jermy', 'level' => -4, 'score' => -22.2, 'altitude' => 1000 },
        { 'character' => 'jerry', 'level' => 73, 'score' => 211.2, 'altitude' => 4581 }
      ]
    end

    let(:output) do
      [
        { 'character' => 'tom',   'level' => 92, 'score' => 220.5, 'altitude' => 5612 },
        { 'character' => 'jerry', 'level' => 73, 'score' => 211.2, 'altitude' => 4581 }
      ]
    end

    it 'raises an error' do
      expect(step.run_with(input)).to match_array output
      expect(File.read(logfile).lines.count).to be > 1
    end
  end
end
