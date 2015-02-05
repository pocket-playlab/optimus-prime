require 'spec_helper'

describe GroupBy do

  let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/transforms.yml") }
  let(:csv_source) { config.get_transform_by_id('games_record_sample') }
  let(:csv_instance) { Csv.new(csv_source['columns'], csv_source['file_path']) }
  let(:key_columns) { [''] }


  context '#initialize' do

    context 'when invalid parameter type' do
      it { expect { GroupBy.new(['array_of_source'], key_columns, {'score' => 'sum'}) }.to raise_error("source must inherit from either OptimusPrime::Source or OptimusPrime::Transform!") }
      it { expect { GroupBy.new(csv_instance, 'key_as_string', {'score' => 'sum'}) }.to raise_error("key_columns should be an array") }
      it { expect { GroupBy.new(csv_instance, key_columns, {'score' => 100}) }.to raise_error("100 not include in strategies") }
      it { expect { GroupBy.new(csv_instance, key_columns, {'score' => 'plus'}) }.to raise_error("plus not include in strategies") }
    end

    context 'when valid parameters' do
      it 'should exact source, key_columns and strategy' do
        group_by = GroupBy.new(csv_instance, key_columns, {'score' => 'sum'})
        expect(group_by.source).to eq(csv_instance)
        expect(group_by.strategies.keys.first).to eq('score')
        expect(group_by.strategies.values.first).to eq('sum')
      end
    end

  end

  context '#sum' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'sum'}) }

    it 'should return sum value' do
      expect(group_by.sum).to eq(16000)
    end

  end

  context '#max' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'max'}) }

    it 'should return primary key of maximum score' do
      expect(group_by.max).to eq(["M", "JuiceCubes", "3", "5000", "0"])
    end

  end

  context '#min' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'min'}) }

    it 'should return primary key of maximum score' do
      expect(group_by.min).to eq(["Rick", "JuiceCubes", "1", "1000", "5"])
    end

  end

   context '#median' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'median'}) }

    it 'should return median value of score' do
      expect(group_by.median).to eq(1000)
    end

  end

  context '#mode' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'mode'}) }

    it 'should return median value of score' do
      expect(group_by.mode).to eq(1000)
    end

  end

  context '#average' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'average'}) }

    it 'should return average value of score' do
      expect(group_by.average).to eq(2285.714285714286)
    end

  end

  context '#count' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'count'}) }

    it 'should return count value of score' do
      expect(group_by.count).to eq(7)
    end

  end

  context '#group_by' do

  end
end