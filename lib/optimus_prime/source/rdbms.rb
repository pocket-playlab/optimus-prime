module OptimusPrime
  class Source::RDBMS < OptimusPrime::Source
  	attr_accessor :adapter, :db_name, :columns, :query, :sequel_connect

    def initialize(columns, adapter, db_name, query, db_settings)
      raise "columns required" unless columns
      raise "query required" unless query

      super columns

      @query = query
      @adapter = adapter
      @db_name = db_name

      unless db_settings[:local]
        server_connect(db_settings[:username], db_settings[:password], db_settings[:host])
      else
        local_connect
      end

      get_data
    end

  	def execute_query
      raise "each implementation class should have a way to do execute_query"
    end

    def local_connect
      @sequel_connect = Sequel.connect("sqlite://#{@db_name}")
    end

    def server_connect(username, password, host)
      @sequel_connect = Sequel.connect(:adapter => @adapter, :user => username, :host => host, :database => @db_name, :password => password)
    end

    protected

  	def implement_get_data
      array_data = Array.new
  	  self.execute_query.each do |row|
        row_data = Array.new
        row.each do |key, value|
          row_data.push value
        end
        array_data.push row_data
      end
      @data = array_data
    end
  end
end
