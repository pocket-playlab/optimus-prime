require_relative '../optimus_init.rb'

class MySQL < OptimusPrime::Source::RDBMS

  attr_accessor :columns, :db, :query

  def initialize(columns, username, password, host, dbname, query)
    raise 'columns required' unless columns
    raise 'cannot connect database' unless host && username && password && dbname
    raise 'query required' unless query
    @columns = columns
    @query = query

    self.connect(username, password, host, dbname)
  end

  def connect(username, password, host, dbname)
      @db = Sequel.connect(:adapter => 'mysql', :user => username, :host => host, :database => dbname,:password => password)
  end

  def columns
    return @columns
  end

  def execute_query
    begin
      @db[@query]
    rescue => e
      raise e.message
    end
  end

end