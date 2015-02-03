require_relative '../optimus_init.rb'

class MySQL < OptimusPrime::Source::RDBMS

  attr_accessor :columns, :db, :query

  def initialize(columns, host, username, password, database, query)
    raise 'columns required' unless columns
    raise 'cannot connect database' unless host && username && password && database
    raise 'query requried' unless query
    @columns = columns
    @query = query

    self.connect(db_path)
  end

  def connect(host, username, password, database)
    begin
      @db = Sequel.connect(:adapter => 'mysql', :user => username, :host => host, :database => database,:password => password)
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