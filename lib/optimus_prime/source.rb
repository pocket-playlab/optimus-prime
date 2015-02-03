module OptimusPrime
  class Source
    attr_accessor :columns, :column_to_index, :data
    
    def initialize(columns)
      raise "columns parameter must be an array!" unless columns.is_a? Array

      map = {}

      index = 0

      columns.each do |name|
        map[name] = index
        index += 1
      end

      @column_to_index = map
      @columns = columns
    end

    def retrieve_data
      implement_retrieve_data
      check_column
      @data
    end

    def column_to_index(column)
      @column_to_index[column]
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
