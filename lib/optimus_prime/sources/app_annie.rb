require 'json'
require 'rest_client'

module OptimusPrime
  module Sources
    class AppAnnie < OptimusPrime::Source
      def request(method:, path:, api_key:, **options)
        url = "https://api.appannie.com#{path}"
        begin
          response = JSON.parse(
            RestClient.send(method, url, ({ authorization: "Bearer #{api_key}" }).merge(options))
          )
        rescue => e
          raise e.response
        end
      end
    end
  end
end
