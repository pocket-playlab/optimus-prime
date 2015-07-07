require 'json'

module SupportHelpers
  def load_json(file, symbolize = false)
    JSON.parse(File.read(file), symbolize_names: symbolize)
  end
end

RSpec.configure do |config|
  config.include SupportHelpers
end
