module OptimusPrime
  module Modules
    module Persistence
      class Operation

        def initialize(db)
          @db = db
        end

        def create(params)
          table.insert(params)
        end

        def update(params)
          table.where(id: params.delete(:id)).update(params)
        end

        private

        def table
          @table ||= @db[:operations]
        end

      end
    end
  end
end
