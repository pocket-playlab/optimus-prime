module OptimusPrime
  class Destination::RDBMS < OptimusPrime::Destination
  	attr_accessor :columns, :query


    # For prototyping and proof of concept, we will use hand crafted sql queries.
    # In the future, it might (or might not) be valuable to use an ORM (such as activerecord)
    # to automatically generate queries and write data out to database

  	def execute_query(columns, values)
      raise "each implementation class should have a way to do this (most likely through ORM of some type)"
    end

    protected

  	def implement_put_data

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
