module OptimusPrime
  module Sources
    module FlurryHelpers
      # This class should not be instanciated directly.
      # Use FlurryReportDownloader or FlurryReportGenerator depending on
      # your needs.
      # FlurryConnector contains the shared logic for the FlurryReport
      # Downloader and the FlurryReportGenerator.
      # Thanks to this class, requests to Flurry are handled in the same way
      # with the responses being processed in the appropriate child class.
      class FlurryConnector
        attr_reader :url, :logger

        def initialize(logger)
          @output = nil
          @stop = false
          @logger = logger
        end

        def run
          raise 'Abstract Method!'
        end

        protected

        # Loop until we get an output (report url or report data)
        # or if @stop is set to true, for example if the report is not
        # existing at all
        def loop_request
          return nil unless url
          @stop = request! until @output || @stop
          @output
        end

        # Make the request to Flurry
        def request!
          logger.debug "Fetching report data from #{url}"
          handle_response(Net::HTTP.get_response(URI(url)))
        end

        # Check if we're over limit (1 call / second). If we are
        # wait 1 second before letting the loop continue.
        # If we get 200 back, we proceed to parse the response.
        def handle_response(response)
          return sleep_and_log(1) if response.is_a? Net::HTTPTooManyRequests
          return parse_report_response(response) if response.is_a? Net::HTTPOK
          raise "Unhandled HTTP Status: #{response.class}."
        end

        # Check for the content-type. If Json call handle_json_response.
        # If octet-stream call handle_octet_stream_response.
        def parse_report_response(response)
          content_type = response.header['content-type']
          raise "Unknown Response Type: #{content_type}" unless handlers.keys.include?(content_type)
          handlers[content_type].call(response)
        end

        # Define handlers as lamba depending on the response content-type.
        def handlers
          @handlers ||= {
            'application/json' => -> (response) { handle_json_response(response) },
            'application/octet-stream' => -> (response) { handle_octet_stream_response(response) }
          }
        end

        # Should be implemented in the child class.
        def handle_json_response(response)
          raise 'Abstract Method'
        end

        # Should be implemented in the child class.
        def handle_octet_stream_response(response)
          raise 'Abstract Method'
        end

        # Parse octet-stream data
        def parse_octet_stream(data)
          logger.info 'Parsing report'
          Yajl::Parser.parse Zlib::GzipReader.new(StringIO.new(data)).read
        end

        # Sleep for x seconds after logging a fail request.
        # Returns false to tell the loop to keep looping.
        def sleep_and_log(duration)
          logger.debug "Request Failed. Waiting #{duration} seconds before retrying."
          sleep duration
          false
        end
      end
    end
  end
end
