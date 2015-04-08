require 'spec_helper'
require 'optimus_prime/transformers/underscore_key'

RSpec.describe OptimusPrime::Transformers::UnderscoreKey do
  let(:input) do
    [
      { 'name' => 'John', 'this is my size' => 192.3, 'me@me!' => 83.5 },
      { '2015-12-01 (12:15)' => 'Jack', '3@+#a&' => 24 }
    ]
  end

  let(:result) do
    [
      { 'name' => 'John', 'this_is_my_size' => 192.3, 'meme' => 83.5 },
      { '2015_12_01_12_15' => 'Jack', '3_a' => 24 }
    ]
  end

  let(:transformer) { OptimusPrime::Transformers::UnderscoreKey.new }

  context 'Multiple Hashes with different combinations of characters' do
    it 'should return correct formatted versions' do
      output = []
      transformer.output << output
      input.each { |record| transformer.write(record) }
      expect(output).to match_array result
    end
  end
end
