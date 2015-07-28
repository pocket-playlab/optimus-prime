require 'json'
require 'restclient'

module OptimusPrime
  module Utilities
    class CurrencyLayer
      def initialize(access_key:, options: {}, tmp_currency_layer_file: '/tmp/currency.json')
        @currency_layer_file = tmp_currency_layer_file
        download_currency_file(access_key, options) unless File.exist? @currency_layer_file
      end

      def currencies_rate
        currency_layer_file = File.read(@currency_layer_file)
        rename_quote_keys(JSON.parse(currency_layer_file))
      end

      private

      def download_currency_file(access_key, options)
        url_params = parameterize(options.merge(access_key: access_key))
        @url = "http://apilayer.net/api/live?#{url_params}"
        begin
          save_file RestClient.send(:get, @url)
        rescue => e
          raise e
        end
      end

      def save_file(response)
        res = JSON.parse(response)
        if res['success']
          open @currency_layer_file, 'w' do |io|
            io.write response
          end
        else
          raise "Error #{response['error']['code']} : #{response['error']['info']}"
        end
      end

      def rename_quote_keys(params)
        params['quotes'] = params['quotes'].each_with_object({}) do |(key, value), hash|
          hash[key.sub(params['source'], '')] = value
        end
        params
      end

      def parameterize(params)
        URI.escape(params.collect { |k, v| "#{k}=#{v}" }.join('&'))
      end
    end
  end
end
