module OptimusPrime
  module Sources
    module FlurryHelpers
      class FlurryConnector
        attr_reader :logger

        def initialize(logger)
          @output = nil
          @stop = false
          @logger = logger
        end

        def run
          return nil unless @url
          @stop = request! until @output || @stop
          @output
        end

        protected

        def request!
          @logger.debug "Fetching report data from #{@url}"
          response = Net::HTTP.get_response URI(@url)

          handle_response(response)
        end

        def handle_response(response)
          return sleep_and_log(1) if response.is_a? Net::HTTPTooManyRequests
          return parse_report_response(response) if response.is_a? Net::HTTPOK
          raise "Unhandled HTTP Status: #{response.class}."
        end

        def parse_report_response(response)
          content_type = response.header['content-type']
          raise "Unknown Response Type: #{content_type}" unless handlers.keys.include?(content_type)
          handlers[content_type].call(response)
        end

        def handlers
          @handlers ||= {
            'application/json' => -> (response) { handle_json_response(response) },
            'application/octet-stream' => -> (response) { handle_octet_stream_response(response) }
          }
        end

        def handle_json_response(response)
          json_response = Yajl::Parser.parse(response.body)

          return true if json_response['message'] == 'Report not found'
          return sleep_and_log(poll_interval) if json_response['@reportReady'] == 'false'
          raise "Unknown Json Message: #{json_response}"
        end

        def handle_octet_stream_response(response)
          @output = parse_report response.body
        end

        def parse_report(data)
          @logger.info 'Parsing report'
          Yajl::Parser.parse Zlib::GzipReader.new(StringIO.new(data)).read
        end

        def sleep_and_log(duration)
          @logger.debug "Request Failed. Waiting #{duration} seconds before retrying."
          sleep duration
          false
        end
      end
    end
  end
end
