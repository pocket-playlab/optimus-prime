require_relative '../optimus_init.rb'
require_relative '../sources/json.rb'

require 'csv'
class CSV_Destination < OptimusPrime::Destination
  attr_accessor :file_path

  def initialize(source, columns_map, output_path)
    super source, columns_map, output_path
  end

  protected

  def implement_put_data(data)
    header = @columns_map.keys
    CSV.open( @output_path, 'w' ) do |writer|
      writer << header
      data.each do |row|
        line = []
        row.each do |field|
          line.push field
        end
        writer << line
      end
    end
  end
end