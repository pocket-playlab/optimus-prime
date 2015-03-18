require 'spec_helper'

describe String do
  describe '#convert_to' do
    it 'should convert a string to an integer' do
      expect('11'.convert_to 'INTEGER').to eq 11
    end

    it 'should convert a string to a float' do
      expect('55.05'.convert_to 'FLOAT').to eq 55.05
    end

    it 'should convert a string to boolean' do
      expect('true'.convert_to 'BOOLEAN').to be_truthy
      expect('false'.convert_to 'Boolean').to be_falsey
    end

    it 'should not convert a string' do
      str = 'what is this?'
      expect(str.convert_to 'WTT').to eq str
    end
  end
end
