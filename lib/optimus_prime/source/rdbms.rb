module OptimusPrime
  class Source::RDBMS < OptimusPrime::Source
  	attr_accessor :adapter, :username, :password, :host, :dbname, :columns, :query

  	def execute_query
      raise "each implementation class should have a way to do this (most likely through ORM of some type)"
    end

    def connect
      Sequel.connect(:adapter => @adapter, :user => @username, :host => @host, :database => @dbname,:password => @password)
    end

    protected

  	def implement_retrieve_data
      array_data = Array.new
  	  self.execute_query.each do |row|
        row_data = Array.new
        row.each do |key, value|
          row_data.push value
        end
        array_data.push row_data
      end
      @data = array_data
    end
  end
end
