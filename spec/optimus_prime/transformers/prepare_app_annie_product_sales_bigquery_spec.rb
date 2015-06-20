require 'spec_helper'
require 'optimus_prime/transformers/prepare_app_annie_product_sales_bigquery'

describe OptimusPrime::Transformers::PrepareAppAnnieProductSalesBigquery do
  context 'product sales data' do
    let(:input) do
      [JSON.parse(File.read('spec/supports/app_annie/one_page_response.json'))]
    end

    before(:each) do
      @output = []
      extractor = OptimusPrime::Transformers::PrepareAppAnnieProductSalesBigquery.new
      extractor.output << @output
      input.each { |data| extractor.write(data) }
      extractor.finish
    end

    it 'contains the default fields' do
      expect(@output.map(&:keys).flatten.uniq).to match_array [
        'sales_list',
        'currency',
        'market',
        'vertical',
        'iap_sales_list'
      ]
    end

    it 'converts the type of all revenue fields to Float and wrap {} with []' do
      @output.each do |output|
        output['sales_list'].each do |sale|
          expect(sale['revenue']).to match_array [
            {
              'product' => [{ 'downloads' => 0.0, 'refunds' => 0.0 }],
              'iap' => [{ 'sales' => 0.0, 'refunds' => 0.0 }],
              'ad' => 0.0
            }
          ]
        end
        output['iap_sales_list'].each do |iap|
          expect(iap['revenue'][0]['sales']).to be_a Float
          expect(iap['revenue'][0]['refunds']).to be_a Float
        end
      end
    end
  end
end
