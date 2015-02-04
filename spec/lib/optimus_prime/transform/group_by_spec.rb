require 'spec_helper'

describe GroupBy do

  let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/sources.yml") }
  let(:csv_source) { config.get_transform_by_id('csv_transform_test') }
  let(:csv_instance) { Csv.new(csv_source['columns'], csv_source['file_path']) }
  let(:key_columns) { ['id'] }


  context '#initialize' do

    context 'when invalid parameter type' do
      it { expect { GroupBy.new(['array_of_source'], key_columns, {'event_value' => 'sum'}) }.to raise_error("source must inherit from either OptimusPrime::Source or OptimusPrime::Transform!") }
      it { expect { GroupBy.new(csv_instance, 'key_as_string', {'event_value' => 'sum'}) }.to raise_error("key_columns should be an array") }
      it { expect { GroupBy.new(csv_instance, key_columns, {'event_value' => 100}) }.to raise_error("100 not include in strategies") }
      it { expect { GroupBy.new(csv_instance, key_columns, {'event_value' => 'plus'}) }.to raise_error("plus not include in strategies") }
    end

    context 'when valid parameters' do
      it 'should exact source, key_columns and strategy' do
        group_by = GroupBy.new(csv_instance, key_columns, {'event_value' => 'sum'})
        expect(group_by.source).to eq(csv_instance)
        expect(group_by.strategies.keys.first).to eq('event_value')
        expect(group_by.strategies.values.first).to eq('sum')
      end
    end

  end

  context '#sum' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'event_value' => 'sum'}) }

    it 'should return sum value' do
      expect(group_by.sum).to eq(1409)
    end

  end

  context '#max' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'event_value' => 'max'}) }

    it 'should return primary key of maximum event_value' do
      expect(group_by.max).to eq(9.to_s)
    end

  end

  context '#min' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'event_value' => 'min'}) }

    it 'should return primary key of maximum event_value' do
      expect(group_by.min).to eq(23.to_s)
    end

  end
end