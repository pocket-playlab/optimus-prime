require_relative '../optimus_init.rb'

require 'json'

class Json < OptimusPrime::Source

  attr_accessor :columns, :file_path, :table_data

  def initialize(columns, file_path)
    @columns = columns
    json_file = File.file?(file_path)
    @file_path = file_path
    @table_data = Array.new
    raise "file not found" unless json_file
  end

  def columns
    @columns
  end

  def retrieve_data
    file = File.read(@file_path)
    content = JSON.parse(file)
    table_data = Array.new
    content.each do |item|
      next if item.nil?
      array_item = Array.new
      columns.each do |key, value|
        array_item.push item[key.to_s]
      end
      @table_data.push array_item
    end
    @table_data
  end
end