require 'csv'
require 'rest_client'
require 'yajl'  # to improve performance and reduce memory usage

require 'optimus_prime/sources/flurry_helpers/flurry_connector'
require 'optimus_prime/sources/flurry_helpers/flurry_report'
require 'optimus_prime/sources/flurry_helpers/flurry_report_downloader'
require 'optimus_prime/sources/flurry_helpers/flurry_report_generator'
require 'optimus_prime/sources/flurry_helpers/flurry_session'

module OptimusPrime
  module Sources
    class Flurry < OptimusPrime::Source
      attr_reader :api_access_code, :api_key, :start_time, :end_time, :poll_interval

      def initialize(api_access_code:, api_key:, start_time:, end_time:,
                     poll_interval: 10, report_uri: nil, retry_interval: 600)
        @api_access_code = api_access_code
        @api_key         = api_key
        @start_time      = Time.parse start_time
        @end_time        = Time.parse end_time
        @poll_interval   = poll_interval
        @retry_interval  = retry_interval
        @report_uri      = report_uri
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

      def report
        @report = report_from_uri(@report_uri)
        @report ||= report_from_uri(report_generator)
        FlurryHelpers::FlurryReport.new(@report)
      end

      def report_from_uri(uri)
        FlurryHelpers::FlurryReportDownloader.new(uri, poll_interval, logger).run
      end

      def report_generator
        FlurryHelpers::FlurryReportGenerator.new(@api_access_code, @api_key, @start_time, @end_time, @retry_interval, logger).run
      end
    end
  end
end
