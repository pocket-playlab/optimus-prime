require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::ConstantCamelKey do
  let(:step) { OptimusPrime::Transformers::ConstantCamelKey.new }

  context 'Multiple Hashes with different combinations of characters' do
    let(:input) do
      [
        { 'name' => 'John', 'this is my-size' => 192.3, 'me@me!' => 83.5 },
        { 'under_check_score' => 'Jack', '3@+#a&' => 24, 'number (unique users)' => 1 },
        { 'This 1 tests' => 'nothing', 'I love 555' => 'numbers' },
        { 'Yes, (braces) will' => 'stay', 'this-Is' => 'interesting' },
        { 'whatHAPPENS' => 'with Uppercase' }
      ]
    end

    let(:output) do
      [
        { 'Name' => 'John', 'ThisIsMySize' => 192.3, 'MeMe' => 83.5 },
        { 'UnderCheckScore' => 'Jack', '3A' => 24, 'NumberUniqueUsers' => 1 },
        { 'This1Tests' => 'nothing', 'ILove555' => 'numbers' },
        { 'YesBracesWill' => 'stay', 'ThisIs' => 'interesting' },
        { 'WhatHAPPENS' => 'with Uppercase' }

      ]
    end

    it 'return correct formatted versions' do
      expect(step.run_with(input)).to match_array output
    end
  end
end
