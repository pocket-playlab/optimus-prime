require 'csv'
require 'rest_client'
require 'yajl'  # to improve performance and reduce memory usage

module OptimusPrime
  module Sources
    class Flurry < OptimusPrime::Source
      attr_reader :api_access_code, :api_key, :start_time, :end_time, :poll_interval

      def initialize(api_access_code:, api_key:, start_time:, end_time:, poll_interval: 10)
        @api_access_code = api_access_code
        @api_key         = api_key
        @start_time      = Time.parse start_time
        @end_time        = Time.parse end_time
        @poll_interval   = poll_interval
        raise ArgumentError.new 'start time >= end time' if @start_time >= @end_time
      end

      # Request the report, and poll until it's ready
      def each
        report = request_report
        report_uri = report.fetch('report').fetch('@reportUri')
        report_data = poll report_uri
        process_events(report_data) { |event| yield event }
      end

      private

      # This makes the initial request for the report. The data is not
      # returned. Only a json response is returned. The response should have
      # the url to retrieve the actual data from the request.
      def request_report
        url = request_report_url
        logger.debug "Requesting report from #{url}"
        response = Net::HTTP.get_response URI(url)
        logger.debug response.body
        Yajl::Parser.parse response.body
      end

      # Build the url to make a request for a report to. This is not the same
      # url as the report data itself. The result of the request to this url
      # will be a json hash containing the url to retrieve the actual data
      def request_report_url
        params = {
          apiAccessCode: api_access_code,
          apiKey:        api_key,
          startTime:     start_time.to_i * 1000,
          endTime:       end_time.to_i * 1000,
        }

        query_string = params.map { |k, v| "#{k}=#{v}" }.join('&')

        "http://api.flurry.com/rawData/Events?#{query_string}"
      end

      # This method goes out to retrieve the data that resulted from the query.
      # It will continue to poll the url until all attempts (configurable) have
      # been reached, at which point it will raise an exception.
      def wait_for_report(uri)
        response = fetch_report uri
        case response.header['content-type']
        when 'application/json'
          raise 'Unknown response' if Yajl::Parser.parse(response.body)['@reportReady'] != 'false'
          poll uri
        when 'application/octet-stream'
          parse_report response.body
        else
          raise 'Unknown response'
        end
      end

      def poll(uri)
        sleep poll_interval
        wait_for_report uri
      end

      def fetch_report(uri)
        logger.debug "Fetching report data from #{uri}"
        Net::HTTP.get_response URI(uri)
      end

      def parse_report(data)
        logger.info 'Parsing report'
        Yajl::Parser.parse Zlib::GzipReader.new(StringIO.new(data)).read
      end

      # Given an events report, yield a hash containing session and event data
      # for each event in the report.
      def process_events(report_data)
        meta = report_data['meta']

        # Expand hash keys into their full names for readability
        sessions = report_data['sessionEvents'].lazy.map(&expand_keys = lambda do |hash|
          hash.map { |k, v| [meta[k], v.is_a?(Array) ? v.map(&expand_keys) : v] }.to_h
        end)

        sessions.each do |session|
          extract_events(session) { |event| yield event }
        end
      end

      def extract_events(session)
        session['logs'].each do |event|
          yield format(session, event)
        end
      end

      def format(session, event)
        normalized = {
          'Session'   => session['uniqueId'],
          'Version'   => session['version'],
          'Device'    => session['device'],
          'Event'     => event['eventName'],
          'Timestamp' => (session['startTimestamp'] + event['offsetTimestamp']) / 1000,
        }
        normalized.merge! event['parameters'] if event['parameters']
        normalized
      end
    end
  end
end
