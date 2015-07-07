require 'spec_helper'
require 'date'

RSpec.describe OptimusPrime::Transformers::CastType do
  let(:type_map_correct) do
    {
      amount: 'integer',
      # Testing that case is ignored
      price: 'Float',
      is_available: 'BOOLEAN',
      notes: 'string',
      due: 'date',
      timestamp: 'datetime',
      a: 'boolean',
      b: 'boolean',
      c: 'boolean',
      d: 'boolean',
      e: 'integer'
    }
  end

  let(:type_map_erroneous) { { amount: 'integer', price: 'lorem' } }
  let(:logfile) { '/tmp/cast_type.log' }
  let(:logger) { Logger.new(logfile) }

  let(:input_valid) do
    [
      {
        'event' => 'buymeat',
        'amount' => '23',
        'price' => '299.23',
        'timestamp' => 1429660574,
        'is_available' => 'False',
        'notes' => 'lorem ipsum',
        'field' => 'not affected'
      },
      {
        'event' => 'buybeans',
        'amount' => '125',
        'price' => '412.5',
        'due' => '2015-04-01',
        'is_available' => 'true',
        'notes' => 5,
        'a' => 'false',
        'b' => 'yes',
        'c' => 'no',
        'd' => 'something',
        'e' => 10.33
      },
      {
        'price' => nil
      }
    ]
  end

  let(:output_valid) do
    [
      {
        'event' => 'buymeat',
        'amount' => 23,
        'price' => 299.23,
        'timestamp' => Time.at(1429660574).to_datetime,
        'is_available' => false,
        'notes' => 'lorem ipsum',
        'field' => 'not affected'
      },
      {
        'event' => 'buybeans',
        'amount' => 125,
        'price' => 412.5,
        'due' => Date.parse('2015-04-01'),
        'is_available' => true,
        'notes' => '5',
        'a' => false,
        'b' => true,
        'c' => false,
        'd' => false,
        'e' => 10
      },
      {
        'price' => nil
      }
    ]
  end

  context 'valid input and correct type map' do
    it 'converts each value to it\'s real type' do
      step = OptimusPrime::Transformers::CastType.new(type_map: type_map_correct)
      expect(step.run_with(input_valid)).to match_array output_valid
    end
  end

  context 'valid input and incorrect type map' do
    it 'raises a TypeError exception' do
      step = OptimusPrime::Transformers::CastType.new(type_map: type_map_erroneous)
      expect{ step.run_and_raise(input_valid) }.to raise_error(TypeError)
    end
  end

  context 'invalid input and correct type map' do
    let(:input) do
      [
        { 'event' => 'buymeat',  'amount' => '23',      'price' => '299.23' },
        { 'event' => 'buybeans', 'amount' => 'nothing', 'price' => '412.5'  },
        { 'event' => 'buybeans', 'amount' => '35',      'price' => '333.5'  }
      ]
    end
    let(:output) do
      [
        { 'event' => 'buymeat',  'amount' => 23, 'price' => 299.23 },
        { 'event' => 'buybeans', 'amount' => 35, 'price' => 333.5  }
      ]
    end
    before { File.delete(logfile) if File.exist?(logfile) }
    it 'logs an exception and skip the record' do
      caster = OptimusPrime::Transformers::CastType.new(type_map: type_map_correct).log_to(logger)
      expect(caster.run_with(input)).to match_array output
      expect(File.read(logfile).lines.count).to be > 1
    end
  end
end
