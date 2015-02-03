module OptimusPrime
  class Source
  	attr_accessor :columns, :column_to_index

  	def initialize(columns)
  	  raise "columns parameter must be an array!" unless columns.kind_a? Array

  	  map = {}

  	  index = 0

  	  columns.each do |name|
  	  	map[name] = index
  	  	index++
  	  end

  	  @column_to_index = map
    end

    def retrieve_data
      raise "The 'retrieve_data' method is not defined in subclass! Please define before continuing."
    end

    def column_to_index(column)
      @column_to_index[column]
    end
  end
end
