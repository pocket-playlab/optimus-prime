require_relative '../optimus_init.rb'

class MySQL < OptimusPrime::Source::RDBMS

  attr_accessor :columns, :db, :query

  def initialize(columns, username, password, host, dbname, query)
    raise 'columns required' unless columns
    raise 'cannot connect database' unless host && username && password && dbname
    raise 'query required' unless query
    super columns
    @query = query
    @adapter = 'mysql'

    self.connect
    self.retrieve_data
  end

  def columns
    return @columns
  end

  def execute_query
    @db[@query]
  end

end