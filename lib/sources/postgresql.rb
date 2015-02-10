require_relative '../optimus_init.rb'

class PostgreSQL < OptimusPrime::Source::RDBMS

  def initialize(columns, username, password, host, dbname, query)
    db_settings = {
      :local => false,
      :username => username,
      :password => password,
      :host => host
    }
    super columns, 'postgres', dbname, query, db_settings
  end

  def execute_query
    @sequel_connect[@query]
  end

end