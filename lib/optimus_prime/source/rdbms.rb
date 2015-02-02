module OptimusPrime
  class Source::RDBMS
  	attr_accessor :query

  	def execute_query
      raise "each implementation class should have a way to do this (most likely through ORM of some type)"
    end

  	def retrieve_data
      begin
        array_data = Array.new
    	  self.execute_query.each do |row|
          row_data = Array.new
          row.each do |key, value|
            row_data.push value
          end
          array_data.push row_data
        end
        array_data
      rescue => e
        raise e.message
      end
    end
  end
end