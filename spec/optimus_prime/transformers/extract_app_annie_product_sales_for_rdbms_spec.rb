require 'spec_helper'
require 'optimus_prime/transformers/extract_app_annie_product_sales_for_rdbms'

describe OptimusPrime::Transformers::ExtractAppAnnieProductSalesForRdbms do
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
    @extractor = OptimusPrime::Transformers::ExtractAppAnnieProductSalesForRdbms.new(
      default_fields: ['vertical', 'currency', 'market']
    )
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
          'sales_list_date' => '2015-01-01',
          'sales_list_country' => 'AU',
          'sales_list_units_product_downloads' => 6,
          'sales_list_units_product_refunds' => 0,
          'sales_list_units_iap_sales' => 0,
          'sales_list_units_iap_refunds' => 0,
          'sales_list_revenue_product_downloads' => '0.00',
          'sales_list_revenue_product_refunds' => '0.00',
          'sales_list_revenue_iap_sales' => '0.00',
          'sales_list_revenue_iap_refunds' => '0.00',
          'sales_list_revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios'
        },
        {
          'sales_list_date' => '2015-01-01',
          'sales_list_country' => 'US',
          'sales_list_units_product_downloads' => 2,
          'sales_list_units_product_refunds' => 0,
          'sales_list_units_iap_sales' => 0,
          'sales_list_units_iap_refunds' => 0,
          'sales_list_revenue_product_downloads' => '0.00',
          'sales_list_revenue_product_refunds' => '0.00',
          'sales_list_revenue_iap_sales' => '0.00',
          'sales_list_revenue_iap_refunds' => '0.00',
          'sales_list_revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios'
        },
        {
          'sales_list_date' => '2015-01-01',
          'sales_list_country' => 'TH',
          'sales_list_units_product_downloads' => 0,
          'sales_list_units_product_refunds' => 0,
          'sales_list_units_iap_sales' => 0,
          'sales_list_units_iap_refunds' => 0,
          'sales_list_revenue_product_downloads' => '0.00',
          'sales_list_revenue_product_refunds' => '0.00',
          'sales_list_revenue_iap_sales' => '0.00',
          'sales_list_revenue_iap_refunds' => '0.00',
          'sales_list_revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios'
        },
        {
          'iap_sales_list_date' => '2015-04-18',
          'iap_sales_list_country' => 'AU',
          'iap_sales_list_units_sales' => 1,
          'iap_sales_list_units_refunds' => 0,
          'iap_sales_list_iap' => 'com.app.product_1',
          'iap_sales_list_revenue_sales' => '3.14',
          'iap_sales_list_revenue_refunds' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios'
        },
        {
          'iap_sales_list_date' => '2015-04-18',
          'iap_sales_list_country' => 'US',
          'iap_sales_list_units_sales' => 1,
          'iap_sales_list_units_refunds' => 0,
          'iap_sales_list_iap' => 'com.app.product_2',
          'iap_sales_list_revenue_sales' => '5.55',
          'iap_sales_list_revenue_refunds' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios'
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
          'sales_list_date' => '2015-01-01',
          'sales_list_country' => 'AU',
          'sales_list_units_product_downloads' => 6,
          'sales_list_units_product_refunds' => 0,
          'sales_list_units_iap_sales' => 0,
          'sales_list_units_iap_refunds' => 0,
          'sales_list_revenue_product_downloads' => '0.00',
          'sales_list_revenue_product_refunds' => '0.00',
          'sales_list_revenue_iap_sales' => '0.00',
          'sales_list_revenue_iap_refunds' => '0.00',
          'sales_list_revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios'
        },
        {
          'sales_list_date' => '2015-01-01',
          'sales_list_country' => 'US',
          'sales_list_units_product_downloads' => 2,
          'sales_list_units_product_refunds' => 0,
          'sales_list_units_iap_sales' => 0,
          'sales_list_units_iap_refunds' => 0,
          'sales_list_revenue_product_downloads' => '0.00',
          'sales_list_revenue_product_refunds' => '0.00',
          'sales_list_revenue_iap_sales' => '0.00',
          'sales_list_revenue_iap_refunds' => '0.00',
          'sales_list_revenue_ad' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios'
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
          'iap_sales_list_date' => '2015-04-18',
          'iap_sales_list_country' => 'AU',
          'iap_sales_list_units_sales' => 1,
          'iap_sales_list_units_refunds' => 0,
          'iap_sales_list_iap' => 'com.app.product_1',
          'iap_sales_list_revenue_sales' => '3.14',
          'iap_sales_list_revenue_refunds' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios'
        },
        {
          'iap_sales_list_date' => '2015-04-18',
          'iap_sales_list_country' => 'US',
          'iap_sales_list_units_sales' => 1,
          'iap_sales_list_units_refunds' => 0,
          'iap_sales_list_iap' => 'com.app.product_2',
          'iap_sales_list_revenue_sales' => '5.55',
          'iap_sales_list_revenue_refunds' => '0.00',
          'vertical' => 'apps',
          'currency' => 'USD',
          'market' => 'ios'
        }
      ]
      expect_output(modified_input, expected_output)
    end
  end
end