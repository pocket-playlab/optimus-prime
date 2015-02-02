module Optimus::Prime
  class Source::Rdbms
  	attr_accessor :query

  	def execute_query
  	  # each implementation class should have a way to do this (most likely through ORM of some type)
    end

  	def retrieve_data
  	  self.execute_query
    end
  end
end