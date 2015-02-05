module OptimusPrime
  class Source
    attr_accessor :columns, :index_of_column, :data
    
    def initialize(columns)
      raise "columns parameter must be an Hash!" unless columns.is_a? Hash
      @index_of_column = columns.keys
      @columns = columns
    end

    def retrieve_data
      implement_retrieve_data
      check_column
      @data
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

    protected

    def implement_retrieve_data
      raise "The 'retrieve_data' method is not defined in subclass! Please define before continuing."
    end

    private

    def check_column
      raise "expect #{@columns.count} columns, but received #{@data.first.count}" if @columns.count != @data.first.count
    end

  end
end
