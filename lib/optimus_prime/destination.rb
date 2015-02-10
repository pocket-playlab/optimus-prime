module OptimusPrime
  class Destination
    attr_accessor :source, :columns_map, :output_path

    def initialize(source, columns_map, output_path)
      unless (source.is_a? OptimusPrime::Source or source.is_a? OptimusPrime::Transform)
        raise "source must inherit from either OptimusPrime::Source or OptimusPrime::Transform!"
      end
      @source = source
      @columns_map = columns_map
      @output_path = output_path
    end

    def put_data
      raise "columns not found on source" if columns_incorrect?
      implement_put_data(@source.get_data)
    end

    def column_to_index(columns)
      index_by_request = Array.new
      hash = Hash[@index_of_column.map.with_index.to_a]
      columns.each do |column|
        raise "column #{column} not found" if hash[column].nil?
        index_by_request.push hash[column]
      end
      index_by_request
    end

    def columns_incorrect?
      @source.columns.keys != @columns_map.keys
    end

    protected

    def implement_put_data
      raise "The 'implement_put_data' method is not defined in subclass! Please define before continuing."
    end

  end
end
