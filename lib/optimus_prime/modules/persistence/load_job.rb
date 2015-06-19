module OptimusPrime
  module Modules
    module Persistence
      class LoadJob

        def initialize(db)
          @db = db
        end

        def create(params)
          table.insert(params)
        end

        def update(params)
          table.where(params.delete(:identifier)).update(params)
        end

        def table
          @table ||= @db[:load_jobs]
        end

      end
    end
  end
end
