module OptimusPrime
  module Transformers
    # Adds the given keys and values to each record
    class Annotate < Destination
      def initialize(extra)
        @extra = extra.with_indifferent_access
      end

      def write(record)
        push record.merge(@extra)
      end
    end
  end
end
