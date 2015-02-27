require 'json'
require 'bigquery'

module OptimusPrime
  module Sources
    class Bigquery < OptimusPrime::Source
      attr_reader :records

      def initialize(project_id:, **config_params)
        @records = []
        @project_id = project_id
        setup **config_params
        GoogleBigquery::Auth.new.authorize
      end

      def each
        @records.each { |record| yield record }
      end

      private

      def setup(pass_phrase:, key_file:, scope:, email:, retries:)
        GoogleBigquery::Config.setup do |config|
          config.pass_phrase = pass_phrase
          config.key_file    = key_file
          config.scope       = scope
          config.email       = email
          config.retries     = retries
        end
      end
    end
  end
end