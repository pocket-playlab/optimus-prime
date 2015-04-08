module OptimusPrime
  module Sources
    module FlurryHelpers
      class FlurryReport
        include Enumerable

        def initialize(report)
          @report = report
        end

        def each
          @report['sessionEvents'].each do |session|
            yield FlurrySession.new session, meta: @report['meta']
          end
        end
      end
    end
  end
end
