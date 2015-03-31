module OptimusPrime
  module Sources
    module FlurryHelpers
      class FlurryReportDownloader < FlurryConnector
        attr_reader :report_uri, :poll_interval

        def initialize(report_uri, poll_interval, logger)
          @url = report_uri
          @poll_interval = poll_interval
          @logger = logger
        end
      end
    end
  end
end
