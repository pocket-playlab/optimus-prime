module OptimusPrime
  module Sources
    module FlurryHelpers
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
end
