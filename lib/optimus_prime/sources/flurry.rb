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

      def each
        report.each do |session|
          session.each do |event|
            yield event
          end
        end
      end

      private

      # Request the report, and poll until it's ready
      def report
        report = request_report
        report_uri = report.fetch('report').fetch('@reportUri')
        report_data = poll report_uri
        FlurryReport.new report_data
      end

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
    end

    class FlurryReport
      include Enumerable

      def initialize(report)
        @report = report
      end

      def each
        @report['sessionEvents'].lazy.map do |session|
          FlurrySession.new session, meta: @report['meta']
        end
      end
    end

    class FlurrySession
      include Enumerable

      def initialize(session, meta:)
        # Expand hash keys into their full names for readability
        expand_keys = lambda do |hash|
          hash.map { |k, v| [meta[k], v.is_a?(Array) ? v.map(&expand_keys) : v] }.to_h
        end
        @session = expand_keys.call session
      end

      def each
        @session['logs'].each do |event|
          yield format event
        end
      end

      private

      def format(event)
        normalized = {
          'Session'   => @session['uniqueId'],
          'Version'   => @session['version'],
          'Device'    => @session['device'],
          'Event'     => event['eventName'],
          'Timestamp' => (@session['startTimestamp'] + event['offsetTimestamp']) / 1000,
        }
        normalized.merge! event['parameters'] if event['parameters']
        normalized
      end
    end
  end
end
