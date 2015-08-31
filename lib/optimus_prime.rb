require 'sequel'
Sequel.extension :migration, :core_extensions

require 'active_support/core_ext/hash'

# Persistence
require 'optimus_prime/modules/persistence/operation'
require 'optimus_prime/modules/persistence/load_job'
require 'optimus_prime/modules/persistence/listener'
require 'optimus_prime/modules/persistence/base'

# Exceptional
require 'optimus_prime/modules/exceptional/adapters/base_adapter.rb'
require 'optimus_prime/modules/exceptional/adapters/sentry_adapter.rb'

require 'optimus_prime/pipeline'
require 'optimus_prime/step'
require 'optimus_prime/source'
require 'optimus_prime/destination'

require 'optimus_prime/sources/s3_source'
require 'optimus_prime/destinations/rdbms_writer'
require 'optimus_prime/destinations/s3_destination'
require 'optimus_prime/sources/flurry_helpers/flurry_connector'
require 'optimus_prime/transformers/expand_json'
require 'optimus_prime/sources/app_annie'
require 'optimus_prime/sources/rdbms'

# Load all Sources and Destinations
Dir[File.dirname(__FILE__) + '/optimus_prime/**/*.rb'].each do |file|
  require file
end

# Load all extend classes
Dir[File.dirname(__FILE__) + '/core_ext/*.rb'].each do |file|
  require file
end

module OptimusPrime
  def self.root
    File.dirname __dir__
  end
end
