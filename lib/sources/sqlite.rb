require_relative '../optimus_init.rb'

class Sqlite < OptimusPrime::Source::RDBMS

  attr_accessor :columns, :db, :db_path

  def initialize(columns, db_path, query)
    raise 'columns, db_path and query are required' unless columns && db_path && query
    @columns = columns
    @query = query

    self.connect(db_path)
  end

  def connect(db_path)
    begin
      @db = Sequel.connect("sqlite://#{@db_path}")
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