require 'spec_helper'
require 'date'
require 'optimus_prime/transformers/cast_type'

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
      d: 'boolean'
    }
  end

  let(:type_map_erroneous) { { amount: 'integer', price: 'lorem' } }
  let(:logfile) { '/tmp/cast_string.log' }
  let(:logger) { Logger.new(logfile) }

  let(:input_valid) do
    [
      {
        event: 'buymeat',
        amount: '23',
        price: '299.23',
        timestamp: 1429660574,
        is_available: 'False',
        notes: 'lorem ipsum',
        field: 'not affected'
      },
      {
        event: 'buybeans',
        amount: '125',
        price: '412.5',
        due: '2015-04-01',
        is_available: 'true',
        notes: 5,
        a: 'false',
        b: 'yes',
        c: 'no',
        d: 'something'
      },
      {
        price: nil
      }
    ]
  end

  let(:output_valid) do
    [
      {
        event: 'buymeat',
        amount: 23,
        price: 299.23,
        timestamp: Time.at(1429660574).to_datetime,
        is_available: false,
        notes: 'lorem ipsum',
        field: 'not affected'
      },
      {
        event: 'buybeans',
        amount: 125,
        price: 412.5,
        due: Date.parse('2015-04-01'),
        is_available: true,
        notes: '5',
        a: false,
        b: true,
        c: false,
        d: false
      },
      {
        price: nil
      }
    ]
  end

  let(:input_invalid) do
    [
      { event: 'buymeat',  amount: '23',      price: '299.23' },
      { event: 'buybeans', amount: 'nothing', price: '412.5'  },
      { event: 'buybeans', amount: '35',      price: '333.5'  }
    ]
  end

  let(:output_invalid) do
    [
      { event: 'buymeat',  amount: 23, price: 299.23 },
      { event: 'buybeans', amount: 35, price: 333.5  }
    ]
  end

  context 'valid input and correct type map' do
    it 'should successfully convert each value to it\'s real type' do
      caster = OptimusPrime::Transformers::CastType.new(type_map: type_map_correct)
      caster.logger = logger
      output = []
      caster.output << output
      input_valid.each { |record| caster.write(record) }
      expect(output).to match_array output_valid
    end
  end

  context 'valid input and incorrect type map' do
    it 'should raise a TypeError exception' do
      caster = OptimusPrime::Transformers::CastType.new(type_map: type_map_erroneous)
      caster.logger = logger
      expect { input_valid.each { |record| caster.write(record) } }.to raise_error(TypeError)
    end
  end

  context 'invalid input and correct type map' do
    before { File.delete(logfile) }
    it 'should log an exception and skip the record' do
      caster = OptimusPrime::Transformers::CastType.new(type_map: type_map_correct)
      caster.logger = logger
      output = []
      caster.output << output
      input_invalid.each { |record| caster.write(record) }
      expect(output).to match_array output_invalid
      expect(File.read(logfile).lines.count).to be > 1
    end
  end
end
