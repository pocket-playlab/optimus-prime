require 'spec_helper'
require 'optimus_prime/transformers/constant_camel_key'

RSpec.describe OptimusPrime::Transformers::ConstantCamelKey do
  let(:transformer) { OptimusPrime::Transformers::ConstantCamelKey.new }

  context 'Multiple Hashes with different combinations of characters' do
    let(:input) do
      [
        { 'name' => 'John', 'this is my size' => 192.3, 'me@me!' => 83.5 },
        { 'under_check_score' => 'Jack', '3@+#a&' => 24, 'number (unique users)' => 1 },
        { 'This 1 tests' => 'nothing', 'I love 555' => 'numbers' },
        { 'Yes, (braces) get' => 'removed' }
      ]
    end

    let(:result) do
      [
        { 'Name' => 'John', 'ThisIsMySize' => 192.3, 'MeMe' => 83.5 },
        { 'UnderCheckScore' => 'Jack', '3A' => 24, 'Number' => 1 },
        { 'This1Tests' => 'nothing', 'ILove555' => 'numbers' },
        { 'YesGet' => 'removed' }
      ]
    end

    it 'should return correct formatted versions' do
      output = []
      transformer.output << output
      input.each { |record| transformer.write(record) }
      expect(output).to match_array result
    end
  end
end
