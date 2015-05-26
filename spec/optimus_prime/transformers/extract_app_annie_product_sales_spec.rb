require 'spec_helper'
require 'optimus_prime/transformers/extract_app_annie_product_sales'

describe OptimusPrime::Transformers::ExtractAppAnnieProductSales do
  let(:multiple_pages_input) do
    [
      JSON.parse(File.read('spec/supports/app_annie/one_page_response.json')),
      JSON.parse(File.read('spec/supports/app_annie/response_2.json'))
    ]
  end

  let(:one_page_input) do
    [JSON.parse(File.read('spec/supports/app_annie/one_page_response.json'))]
  end

  before(:each) do
    @output = []
    @extractor = OptimusPrime::Transformers::ExtractAppAnnieProductSales.new
    @extractor.output << @output
  end

  def expect_output(input, expected_output)
    input.each { |data| @extractor.write(data) }
    @extractor.finish
    expect(@output).to match_array expected_output
  end

  context 'input contains both sales_list and iap_sales_list from multiple pages' do
    it 'returns an array of extracted data' do
      expected_output = [
        {
          'date' => '2015-01-01',
          'country' => 'AU',
          'units_product_downloads' => 6,
          'units_product_refunds' => 0,
          'units_iap_sales' => 0,
          'units_iap_refunds' => 0,
          'revenue_product_downloads' => '0.00',
          'revenue_product_refunds' => '0.00',
          'revenue_iap_sales' => '0.00',
          'revenue_iap_refunds' => '0.00',
          'revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios',
          'sales_type' => 'sales_list'
        },
        {
          'date' => '2015-01-01',
          'country' => 'US',
          'units_product_downloads' => 2,
          'units_product_refunds' => 0,
          'units_iap_sales' => 0,
          'units_iap_refunds' => 0,
          'revenue_product_downloads' => '0.00',
          'revenue_product_refunds' => '0.00',
          'revenue_iap_sales' => '0.00',
          'revenue_iap_refunds' => '0.00',
          'revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios',
          'sales_type' => 'sales_list'
        },
        {
          'date' => '2015-01-01',
          'country' => 'TH',
          'units_product_downloads' => 0,
          'units_product_refunds' => 0,
          'units_iap_sales' => 0,
          'units_iap_refunds' => 0,
          'revenue_product_downloads' => '0.00',
          'revenue_product_refunds' => '0.00',
          'revenue_iap_sales' => '0.00',
          'revenue_iap_refunds' => '0.00',
          'revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios',
          'sales_type' => 'sales_list'
        },
        {
          'date' => '2015-04-18',
          'country' => 'AU',
          'units_sales' => 1,
          'units_refunds' => 0,
          'iap' => 'com.app.product_1',
          'revenue_sales' => '3.14',
          'revenue_refunds' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios',
          'sales_type' => 'iap_sales_list'
        },
        {
          'date' => '2015-04-18',
          'country' => 'US',
          'units_sales' => 1,
          'units_refunds' => 0,
          'iap' => 'com.app.product_2',
          'revenue_sales' => '5.55',
          'revenue_refunds' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios',
          'sales_type' => 'iap_sales_list'
        }
      ]
      expect_output(multiple_pages_input, expected_output)
    end
  end

  context 'input contains only sales_list from one page' do
    it 'returns an array of extracted data' do
      modified_input = one_page_input
      modified_input[0]['iap_sales_list'] = []

      expected_output = [
        {
          'date' => '2015-01-01',
          'country' => 'AU',
          'units_product_downloads' => 6,
          'units_product_refunds' => 0,
          'units_iap_sales' => 0,
          'units_iap_refunds' => 0,
          'revenue_product_downloads' => '0.00',
          'revenue_product_refunds' => '0.00',
          'revenue_iap_sales' => '0.00',
          'revenue_iap_refunds' => '0.00',
          'revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios',
          'sales_type' => 'sales_list'
        },
        {
          'date' => '2015-01-01',
          'country' => 'US',
          'units_product_downloads' => 2,
          'units_product_refunds' => 0,
          'units_iap_sales' => 0,
          'units_iap_refunds' => 0,
          'revenue_product_downloads' => '0.00',
          'revenue_product_refunds' => '0.00',
          'revenue_iap_sales' => '0.00',
          'revenue_iap_refunds' => '0.00',
          'revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios',
          'sales_type' => 'sales_list'
        }
      ]
      expect_output(modified_input, expected_output)
    end
  end

  context 'input contains only iap_sales_list from one page' do
    it 'returns an array of extracted data' do
      modified_input = one_page_input
      modified_input[0]['sales_list'] = []

      expected_output = [
        {
          'date' => '2015-04-18',
          'country' => 'AU',
          'units_sales' => 1,
          'units_refunds' => 0,
          'iap' => 'com.app.product_1',
          'revenue_sales' => '3.14',
          'revenue_refunds' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios',
          'sales_type' => 'iap_sales_list'
        },
        {
          'date' => '2015-04-18',
          'country' => 'US',
          'units_sales' => 1,
          'units_refunds' => 0,
          'iap' => 'com.app.product_2',
          'revenue_sales' => '5.55',
          'revenue_refunds' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios',
          'sales_type' => 'iap_sales_list'
        }
      ]
      expect_output(modified_input, expected_output)
    end
  end
end
