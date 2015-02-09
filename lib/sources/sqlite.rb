require_relative '../optimus_init.rb'

class Sqlite < OptimusPrime::Source::RDBMS

  attr_accessor :columns, :db, :query

  def initialize(columns, db_path, query)
    raise 'columns, db_path and query are required' unless columns && db_path && query
    super columns
    @query = query

    self.connect(db_path)
  end

  def connect(db_path)
    @db = Sequel.connect("sqlite://#{db_path}")
  end

  def execute_query
    @db[@query]
  end
end