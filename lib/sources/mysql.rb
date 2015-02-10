require_relative '../optimus_init.rb'

class MySQL < OptimusPrime::Source::RDBMS
  def initialize(columns, username, password, host, db_name, query)
    raise 'columns required' unless columns
    raise 'query required' unless query
    db_settings = {
      :local => false,
      :username => username,
      :password => password,
      :host => host
    }
    super columns, 'mysql', db_name, query, db_settings
  end

  def execute_query
    @sequel_connect[@query]
  end

end