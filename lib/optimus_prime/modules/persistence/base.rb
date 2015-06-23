module OptimusPrime
  module Modules
    module Persistence
      class Base
        attr_reader :options, :db

        def initialize(dsn:)
          @db = Sequel.connect(dsn)
          run_migrations
        end

        def run_migrations
          Sequel::Migrator.run(@db, 'migrations')
        end

        def operation
          @operation ||= Operation.new(@db)
        end

        def load_job
          @load_job ||= LoadJob.new(@db)
        end

        def listener
          @listener ||= Listener.new(self)
        end
      end
    end
  end
end
