require 'spec_helper'
require 'optimus_prime/transformers/validator'

RSpec.describe OptimusPrime::Transformers::Validator do

  let(:constraints) do
    {
      'level' => {
        'type'   => 'range',
        'values' => [0, 100]
      },
      'score' => {
        'type'   => 'greater_than_or_equal',
        'values' => [0]
      },
      'altitude' => {
        'type'   => 'less_than_or_equal',
        'values' => [8888]
      },
      'character' => {
        'type'   => 'set',
        'values' => ['tom', 'jerry']
      }
    }
  end

  let(:logfile) { '/tmp/validator.log' }

  let(:validator) do
    validator = OptimusPrime::Transformers::Validator.new(constraints: constraints)
    validator.logger = Logger.new(logfile)
    validator
  end

  let(:input_legal) do
    [
      { 'character' => 'tom',   'level' => 92, 'score' => 220.5, 'altitude' => 5612 },
      { 'character' => 'jerry', 'level' => 73, 'score' => 211.2, 'altitude' => 4581 },
      { 'character' => 'jerry', 'level' => 22, 'score' => 111.2, 'altitude' => 2345 }
    ]
  end

  let(:input_illegal) do
    [
      { 'character' => 'tom',   'level' => 92, 'score' => 220.5, 'altitude' => 5612 },
      { 'character' => 'jermy', 'level' => -4, 'score' => -22.2, 'altitude' => 1000 },
      { 'character' => 'jerry', 'level' => 73, 'score' => 211.2, 'altitude' => 4581 }
    ]
  end

  let(:output_illegal) do
    [
      { 'character' => 'tom',   'level' => 92, 'score' => 220.5, 'altitude' => 5612 },
      { 'character' => 'jerry', 'level' => 73, 'score' => 211.2, 'altitude' => 4581 }
    ]
  end

  context 'legal input' do
    it 'should allow all the records to pass away' do
      output = []
      validator.output << output
      input_legal.each { |record| validator.write(record) }
      expect(output).to match_array input_legal
    end
  end

  context 'illegal input' do
    before { File.delete(logfile) }
    it 'should raise an error' do
      output = []
      validator.output << output
      input_illegal.each { |rec| validator.write(rec) }
      expect(output).to match_array output_illegal
      expect(File.read(logfile).lines.count).to be > 1
    end
  end
end
