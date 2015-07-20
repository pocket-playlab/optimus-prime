require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::ChangeValueWithKey do
  let(:mapper) do
    {
      'keyA' => 100,
      'keyB' => 200,
      'keyC' => 'string',
      'keyD' => true
    }
  end

  let(:input) do
    [
      {
        'keyA' => nil,
        'keyB' => 500,
        'untouch' => 'not change'
      },
      {
        'keyA' => 200,
        'keyC' => 100,
        'keyD' => false,
        'untouch' => 'not change'
      }
    ]
  end

  let(:output) do
    [
      {
        'keyA' => 100,
        'keyB' => 200,
        'untouch' => 'not change'
      },
      {
        'keyA' => 100,
        'keyC' => 'string',
        'keyD' => true,
        'untouch' => 'not change'
      }
    ]
  end

  context 'map with different data types' do
    it 'changes everything correctly' do
      step = OptimusPrime::Transformers::ChangeValueWithKey.new(mapper: mapper)
      expect(step.run_with(input)).to match_array output
    end
  end
end
