module OptimusPrime
  module Modules
    module Persistence
      class LoadJob
        def initialize(db)
          @db = db
        end

        def get(identifier)
          table.where(identifier: identifier).order(:start_time).last
        end

        def create(params)
          table.insert(params)
        end

        def update(params)
          table.where(id: params.delete(:id),
                      operation_id: params.delete(:operation_id)).update(params)
        end

        private

        def table
          @table ||= @db[:load_jobs]
        end
      end
    end
  end
end
