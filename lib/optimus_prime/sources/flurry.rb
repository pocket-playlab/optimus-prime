require 'csv'
require 'rest_client'
# using yajl
require 'yajl'

module OptimusPrime
  module Sources
    class Flurry < OptimusPrime::Source
      attr_accessor :start_date, :end_date

      def initialize(api_access_code:,
                     api_key:,
                     end_date: Time.now.utc.to_date,
                     start_date: nil,
                     poll_interval: 10,
                     number_of_days: 1)
        @api_access_code        = api_access_code
        @api_key                = api_key

        if end_date.nil?
          @end_date             = end_date
        else
          @end_date             = Date.parse(end_date)
        end

        @poll_interval          = poll_interval.to_i
        @number_of_days         = number_of_days

        if start_date.nil?
          @start_date           = @end_date.to_date - @number_of_days
        else
          @start_date           = Date.parse(start_date)

          # validate that start date is before end date
          if @start_date <= @end_date
            raise "Start date (#{@start_date}) must be before the end date (#{@end_date})"
          end
        end
      end

      #
      # Build the url to make a request for a report to
      # This is not the same url as the report data itself
      # The result of the request to this url will be a json hash
      # containing the url to retrieve the actual data
      #
      def request_report_url
        params = {
          apiAccessCode: @api_access_code,
          apiKey:        @api_key,
          startTime:     @start_date.to_time.to_i * 1000,
          endTime:       @end_date.to_time.to_i * 1000,
        }

        query_string = params.map { |k, v| "#{k}=#{v}" }.join('&')

        "http://api.flurry.com/rawData/Events?#{query_string}"
      end

      #
      # Implement the each method for OptimusPrime::Source
      #
      def each
        get_data do |row|
          yield row
        end
      end

      private

      # basically execute same thing as the upload_reports_to_s3 method fetch_reports.rb
      # with the exception that there is no S3 related code. Only concerned about getting the data.
      # request_report
      # and keep on trying to retrieve_report
      # parse the thing and turn it into a Enumerable type object
      def get_data
        start_time = @start_date.to_time.utc
        end_time   = @end_date.to_time.utc

        report = request_report(start_time, end_time)
        unless report.key? 'report'
          dump = JSON.dump report
          logger.error dump
          raise 'failed to receive report json!'
        end

        report_uri = report['report']['@reportUri']
        unless report_uri
          logger.error 'No report uri'
          raise 'No report uri!'
        end

        sleep @poll_interval # Wait so we don't get rate-limited
        report_data = get_report_data(report_uri)

        if report_data.nil?
          logger.error "Missing data for interval: #{start_time} - #{end_time}"
        else
          hashes = time_call('Processing events') { process_events report_data }
        end

        logger.info('Done')
        hashes
      end

      #
      # This makes the initial request for the report. The data is not returned. Only a json
      # response is returned. The response should have the url to retrieve the actual
      # resulting data from the request.
      #
      def request_report(from, to)
        unless from.is_a? Time and to.is_a? Time and from < to
          raise ArgumentError.new 'Invalid date range'
        end

        url = request_report_url
        logger.debug("Request_report url is #{url}")

        response = Net::HTTP.get_response URI(url)

        content_type = response.header['content-type']

        if content_type != 'application/json'
          logger.error 'Error getting report'
          logger.error 'Headers:'
          logger.error response.header.to_s
          logger.error 'Body:'
          logger.error response.body
          raise 'Request report failed!'
        end

        logger.debug response.body
        Yajl::Parser.parse response.body
      end

      #
      # This method actually goes out to retrieve the data that resulted from the query.
      # It will continue to poll the url until all attempts (configurable) have been reached
      # at which point, it will raise an exception.
      #
      def get_report_data(report_uri, retries=0)
        logger.debug 'Entered get_report_data method'
        response = Net::HTTP.get_response URI(report_uri)
        content_type = response.header['content-type']
        case content_type
        when 'application/json'
          raise 'Unknown response' unless Yajl::Parser.parse(response.body)['@reportReady'] == 'false'
          logger.debug("Report not ready, retrying in #{@poll_interval} seconds")
          sleep @poll_interval
          get_report_data report_uri
        when 'application/octet-stream'
          #self.logger.info "Report ready: #{report_uri}"
          logger.info "Report ready: #{report_uri}"
          # If we get here, SUCCESS!
          Yajl::Parser.parse Zlib::GzipReader.new(StringIO.new(response.body)).read
        else
          headers = response.each_header.map { |k, v| "#{k}: #{v}" }.join("\n")
          logger.error "Response:\n#{headers}\n\n#{response.body[0...1000]}"
          logger.error "Unknown content-type: #{content_type}"
          # Retry 3 times before we let it die
          if retries <= 3
            # Make sure to sleep if we are being rate limited.
            sleep @poll_interval
            get_report_data(report_uri, retries+1)
          else
            nil
          end
        end
      end

      #
      # Given an events report, returns an enumerator that yields a hash containing
      # session and event data for each event in the report.
      #
      def process_events(report_data)
        meta = report_data['meta']

        # Expand hash keys into their full names for readability
        expand_keys = lambda do |hash|
          hash.map { |k, v| [meta[k], v.is_a?(Array) ? v.map(&expand_keys) : v] }.to_h
        end

        Enumerator.new do |enum|
          report_data['sessionEvents'].lazy.map(&expand_keys).each do |session|
            session['logs'].each do |event|
              timestamp = session['startTimestamp'] + event['offsetTimestamp']
              data = {
                'Session'   => session['uniqueId'],
                'Version'   => session['version'],
                'Device'    => session['device'],
                'Event'     => event['eventName'],
                'Timestamp' => Time.strptime(timestamp.to_s, '%Q').utc.to_i,
              }
              data.merge!(event['parameters']) if event['parameters']
              enum.yield(data)
            end
          end
        end
      end

      private

      #
      # Convienence methods
      #

      def format_timing(timing)
        if timing < 1
          "#{timing * 1000} ms"
        else
          "#{timing} s"
        end
      end

      def time_call(str)
        start_time = Time.now
        if block_given?
          result = yield
        end
        end_time = Time.now
        logger.info "#{str}: #{format_timing(end_time - start_time)}"
        result
      end
    end
  end
end
