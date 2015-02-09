require_relative '../optimus_init.rb'

require 'json'

class Json < OptimusPrime::Source

  attr_accessor :columns, :file_path

  def initialize(columns, file_path)
    super columns
    json_file = File.file?(file_path)
    @file_path = file_path
    @data = Array.new
    raise "file not found" unless json_file
  end

  def columns
    @columns
  end

  protected

  def implement_retrieve_data
    file = File.read(@file_path)
    content = JSON.parse(file)
    content.each do |item|
      next if item.nil?
      array_item = Array.new
      columns.each do |key, value|
        array_item.push item[key.to_s]
      end
      @data.push array_item
    end
  end
end