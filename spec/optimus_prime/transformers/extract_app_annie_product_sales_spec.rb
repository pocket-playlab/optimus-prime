require 'spec_helper'

describe OptimusPrime::Transformers::ExtractAppAnnieProductSales do
  let(:basepath) { 'app_annie' }
  let(:step) { OptimusPrime::Transformers::ExtractAppAnnieProductSales.new }

  context 'input contains both sales_list and iap_sales_list from multiple pages' do
    let(:input)  { load_json("#{basepath}/input_multiple.json")  }
    let(:output) { load_json("#{basepath}/output_multiple.json") }

    it 'returns an array of extracted data' do
      expect(step.run_with(input)).to match_array output
    end
  end

  context 'input contains only sales_list from one page' do
    let(:input)  { load_json("#{basepath}/input_sales_list.json")  }
    let(:output) { load_json("#{basepath}/output_sales_list.json") }

    it 'returns an array of extracted data' do
      expect(step.run_with(input)).to match_array output
    end
  end

  context 'input contains only iap_sales_list from one page' do
    let(:input)  { load_json("#{basepath}/input_iap_sales_list.json")  }
    let(:output) { load_json("#{basepath}/output_iap_sales_list.json") }

    it 'returns an array of extracted data' do
      expect(step.run_with(input)).to match_array output
    end
  end
end
