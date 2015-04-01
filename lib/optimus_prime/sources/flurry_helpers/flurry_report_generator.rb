module OptimusPrime
  module Sources
    module FlurryHelpers
      # The FlurryReportGenerator is responsible for connecting to Flurry and
      # asking it to generate a new report. This class handles API Limit thanks to its
      # parent class and report already being generated (code 108). If a report is being generated
      # a sleep will occurs for 10 minutes (600 seconds). Can be changed with retry_interval
      # in the Flurry Source.
      class FlurryReportGenerator < FlurryConnector
        def initialize(api_access_code, api_key, start_time, end_time, retry_interval, logger)
          @api_access_code = api_access_code
          @api_key = api_key
          @start_time = start_time
          @end_time = end_time
          @retry_interval = retry_interval
          @url = report_url
          super(logger)
        end

        def run
          loop_request
        end

        private

        # Parse the response as JSON then check if a report is already being generated.
        # If not, extract the report uri from the response.
        def handle_json_response(response)
          json_response = Yajl::Parser.parse(response.body)

          return sleep_and_log(@retry_interval) if json_response['code'] == '108'
          return extract_report_uri(json_response) if contains_report?(json_response)
          raise "Unknown Json Message: #{json_response}"
        end

        def extract_report_uri(json_response)
          @stop = true
          @output = json_response['report']['@reportUri']
        end

        def contains_report?(json_response)
          json_response['report'] && json_response['report']['@reportUri']
        end

        # Build the url to make a request for a report to. This is not the same
        # url as the report data itself. The result of the request to this url
        # will be a json hash containing the url to retrieve the actual data
        def report_url
          params = {
            apiAccessCode: @api_access_code,
            apiKey:        @api_key,
            startTime:     @start_time.to_i * 1000,
            endTime:       @end_time.to_i * 1000,
          }

          query_string = params.map { |k, v| "#{k}=#{v}" }.join('&')

          "http://api.flurry.com/rawData/Events?#{query_string}"
        end
      end
    end
  end
end
