require_relative '../optimus_init.rb'

require 'csv'

class Csv < OptimusPrime::Source
  attr_accessor :columns, :file_path

  def initialize(columns, file_path)
    @columns = columns
    csv_file = File.file?(file_path)
    @file_path = file_path
    @data = Array.new
    raise "file not found" unless csv_file
  end

  def columns
    return @columns
  end

  protected

  def implement_retrieve_data
    index = 0
    CSV.foreach(@file_path) do |row|
      raise "incorrect column number" if row.count != columns.count
      @data.push row if index != 0
      index += 1
    end
    @data
  end
end