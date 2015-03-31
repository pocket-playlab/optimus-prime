module OptimusPrime
  module Sources
    module FlurryHelpers
      class FlurryReportGenerator < FlurryConnector
        attr_reader :api_access_code, :api_key, :start_time, :end_time, :retry_interval

        def initialize(api_access_code, api_key, start_time, end_time, retry_interval, logger)
          @api_access_code = api_access_code
          @api_key = api_key
          @start_time = start_time
          @end_time = end_time
          @url = report_url
          @retry_interval = retry_interval
          @logger = logger
        end

        private

        def handle_json_response(response)
          json_response = Yajl::Parser.parse(response.body)

          return sleep_and_log(retry_interval) if json_response['code'] == '108'
          return define_report_uri(json_response) if json_response['report'] && json_response['report']['@reportUri']
          raise "Unknown Json Message: #{json_response}"
        end

        def define_report_uri(json_response)
          @stop = true
          @output = json_response['report']['@reportUri']
        end

        # Build the url to make a request for a report to. This is not the same
        # url as the report data itself. The result of the request to this url
        # will be a json hash containing the url to retrieve the actual data
        def report_url
          params = {
            apiAccessCode: api_access_code,
            apiKey:        api_key,
            startTime:     start_time.to_i * 1000,
            endTime:       end_time.to_i * 1000,
          }

          query_string = params.map { |k, v| "#{k}=#{v}" }.join('&')

          "http://api.flurry.com/rawData/Events?#{query_string}"
        end
      end
    end
  end
end
