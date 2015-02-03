require_relative '../optimus_init.rb'

class MySQL < OptimusPrime::Source::RDBMS

  attr_accessor :username, :password, :host, :dbname

  def initialize(columns, username, password, host, dbname, query)
    raise 'columns, username, password, host, dbname and query are required' unless columns &&  username && password && host && db_path && query
    @columns = columns
    @query = query
    @username = username
    @password = password
    @host = host
    @dbname = dbname

    self.connect
  end

  def connect
    begin
      @db = Sequel.connect(@dbname, :user => @username, :password => @password, :host => @host)
    rescue => e
      raise "Can't connect database"
    end
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