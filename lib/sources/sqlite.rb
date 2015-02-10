require_relative '../optimus_init.rb'

class Sqlite < OptimusPrime::Source::RDBMS

  def initialize(columns, db_path, query)
    raise 'columns, db_path and query are required' unless columns && db_path && query
    super columns, 'sqlite', db_path, query, { :local => true }
  end

  def execute_query
    @sequel_connect[@query]
  end
end