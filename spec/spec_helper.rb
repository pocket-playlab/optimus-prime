require 'rspec'
require 'webmock/rspec'
require 'csv'
require 'mysql'

require File.expand_path("../../lib/optimus_prime.rb", __FILE__)
require File.expand_path("../../lib/sources/csv.rb", __FILE__)
require File.expand_path("../../lib/sources/json.rb", __FILE__)
require File.expand_path("../../lib/sources/sqlite.rb", __FILE__)
require File.expand_path("../../lib/sources/appsflyer.rb", __FILE__)
require File.expand_path("../../lib/sources/mysql.rb", __FILE__)
require File.expand_path("../../lib/sources/postgresql.rb", __FILE__)
require File.expand_path("../../lib/destinations/csv_destination.rb", __FILE__)

require File.expand_path("../../lib/optimus_prime/transform/group_by.rb", __FILE__)

WebMock.disable_net_connect!(allow_localhost: true)

RSpec.configure do |c|
  c.mock_with :rspec
end
