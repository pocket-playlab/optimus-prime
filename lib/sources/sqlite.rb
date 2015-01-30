require_relative '../optimus_init'
require_relative '../../datasource/sqlite_sample' #init database

class Sqlite < OptimusPrime::Source

  attr_accessor :columns, :table_data, :data

  def initialize(columns)
    @columns = columns
    @table_data = Array.new
    self.connect
  end

  def connect
    @data = DB[:items]
  end

  def columns
    return @columns
  end

  def retrieve_data
    @data.each do |item|
      array_item = Array.new
      columns.each do |key, value|
        array_item.push item[key.to_sym]
      end
      @table_data.push array_item
    end
    @table_data
  end
end