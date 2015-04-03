module OptimusPrime
  module Sources
    module FlurryHelpers
      # The FlurryReportDownloader is only responsible for polling
      # Flurry to get a report when it's ready.
      class FlurryReportDownloader < FlurryConnector
        def initialize(report_uri, poll_interval, retry_interval, logger)
          @url = report_uri
          @retry_interval = retry_interval
          @poll_interval = poll_interval
          super(logger)
        end

        def run
          loop_request
        end

        private

        # Parse the response as JSON then check if the report is actually
        # existing or not. Stop the loop if the report does not exist, else
        # continue until the report is ready.
        def handle_json_response(response)
          json_response = Yajl::Parser.parse(response.body)

          return report_not_found if json_response['message'] == 'Report not found'
          return sleep_and_log(@poll_interval) if json_response['@reportReady'] == 'false'
          raise "Unknown Json Message: #{json_response}"
        end

        def handle_octet_stream_response(response)
          @output = parse_octet_stream response.body
        end

        def report_not_found
          logger.debug "Report #{url} not found."
          true
        end
      end
    end
  end
end
