module OptimusPrime
  require_relative 'optimus_prime/config'
  require_relative 'optimus_prime/source'
  require_relative 'optimus_prime/transform'
  require_relative 'optimus_prime/destination'

  require_relative 'sources/events_collector'
  require_relative 'sources/appsflyer'

  require_relative 'destinations/csv'
end
