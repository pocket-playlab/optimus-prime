require 'sequel'

require 'optimus_prime/pipeline'
require 'optimus_prime/step'
require 'optimus_prime/source'
require 'optimus_prime/destination'

require 'optimus_prime/destinations/rdbms_writer'

# Load all Sources and Destinations
Dir[File.dirname(__FILE__) + '/optimus_prime/**/*.rb'].each do |file|
  require file
end

# Load all extend classes
Dir[File.dirname(__FILE__) + '/core_ext/*.rb'].each do |file|
  require file
end
