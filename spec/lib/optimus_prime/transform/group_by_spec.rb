require 'spec_helper'

describe GroupBy do

  let(:csv_source) { config.get_source_by_id('csv_transform_test') }
  let(:csv_instance) { Csv.new(csv_source['columns'], csv_source['file_path']) }
  let(:key_columns) { ['country_code'] }


  context '#initialize' do

    context 'when invalid parameter type' do
      it { expect { GroupBy.new(['array_of_source'], key_columns, 'sum') }.to raise_error("source must inherit from either OptimusPrime::Source or OptimusPrime::Transform!") }
      it { expect { GroupBy.new(csv_instance, 'key_as_string', 'sum') }.to raise_error("key_columns should be an array") }
      it { expect { GroupBy.new(csv_instance, key_columns, 100) }.to raise_error("100 strategy not include") }
      it { expect { GroupBy.new(csv_instance, key_columns, 'plus') }.to raise_error("plus strategy not include") }
    end
  end
end