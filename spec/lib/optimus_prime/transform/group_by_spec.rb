require 'spec_helper'

describe GroupBy do

  let(:config) { OptimusPrime::Config.new(file_path: "spec/supports/transforms.yml") }
  let(:csv_source) { config.get_transform_by_id('games_record_sample') }
  let(:csv_instance) { Csv.new(csv_source['columns'], csv_source['file_path']) }
  let(:key_columns) { [] }


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
      expect(group_by.result[['all']]).to eq(16000)
    end

    it 'should return total score by game_name' do
      game_total_score = GroupBy.new(csv_instance, ['game_name'], {'score' => 'sum'})

      expect(game_total_score.result[['JuiceCubes']]).to eq(15000)
      expect(game_total_score.result[['DragonCubes']]).to eq(1000)
    end

    it 'should return total score by game_name and user' do
      game_total_score = GroupBy.new(csv_instance, ['game_name', 'user'], {'score' => 'sum'})

      expect(game_total_score.result[['JuiceCubes', 'M']]).to eq(8000)
      expect(game_total_score.result[['JuiceCubes', 'Rick']]).to eq(7000)
      expect(game_total_score.result[['DragonCubes', 'M']]).to eq(1000)
    end

  end

  context '#max' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'max'}) }

    it 'should return maximum score' do
      expect(group_by.result).to eq(["M", "JuiceCubes", "3", "5000", "0"])
    end

    it 'should return maximum score'



  end

  context '#min' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'min'}) }

    it 'should return primary key of maximum score' do
      expect(group_by.result).to eq(["Rick", "JuiceCubes", "1", "1000", "5"])
    end

  end

   context '#median' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'median'}) }

    it 'should return median value of score' do
      expect(group_by.result).to eq(1000)
    end

  end

  context '#mode' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'mode'}) }

    it 'should return median value of score' do
      expect(group_by.result).to eq(1000)
    end

  end

  context '#average' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'average'}) }

    it 'should return average value of score' do
      expect(group_by.result).to eq(2285.714285714286)
    end

  end

  context '#count' do

    let(:group_by) { GroupBy.new(csv_instance, key_columns, {'score' => 'count'}) }

    it 'should return count value of score' do
      expect(group_by.result).to eq(7)
    end

  end

  context '#group_by' do

    context 'empty group' do
      it 'should return same source.retrieve data but store in 2d-array' do
        group_by_instance = GroupBy.new(csv_instance, [], {'score' => 'sum'})
        expect(group_by_instance.grouped_data.first[1]).to eq(csv_instance.retrieve_data)
      end
    end

    context 'single key_columns' do
      it 'should gropped data into new array by key_columns parameter' do
        group_by_instance = GroupBy.new(csv_instance, ['game_name'], {'score' => 'sum'})

        expect(group_by_instance.grouped_data.count).to eq(2)
        expect(group_by_instance.grouped_data.keys).to match_array([['JuiceCubes'], ['DragonCubes']])

        jc_expected_level = ["1", "1", "2", "3", "2", "3"]

        expect(group_by_instance.grouped_data.values[0].map{ |data| data[2] }).to match_array(jc_expected_level)
        expect(group_by_instance.grouped_data.values[1].map{ |data| data[2] }).to match_array(["1"])
      end
    end

    context 'multiple key_columns' do
      it 'should gropped data into new array by key_columns parameter' do
        group_by_instance = GroupBy.new(csv_instance, ['game_name', 'user'], {'score' => 'sum'})

        expect(group_by_instance.grouped_data.count).to eq(3)

        keys_expected = [['JuiceCubes', 'M'], ['DragonCubes', 'M'], ['JuiceCubes', 'Rick']]
        expect(group_by_instance.grouped_data.keys).to match_array(keys_expected)

        p1_jc_expected_score = ["2000", "1000", "5000"]
        p2_jc_expected_score = ["1000", "1000", "5000"]
        p1_dc_expected_score = ["1000"]

        expect(group_by_instance.grouped_data.values[0].map{ |data| data[3] }).to match_array(p1_jc_expected_score)
        expect(group_by_instance.grouped_data.values[1].map{ |data| data[3] }).to match_array(p2_jc_expected_score)
        expect(group_by_instance.grouped_data.values[2].map{ |data| data[3] }).to match_array(p1_dc_expected_score)
      end
    end

  end

  context 'complex report' do
  end
end