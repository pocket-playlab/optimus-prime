require_relative '../optimus_init.rb'

class Sprintf < OptimusPrime::Transform

  attr_accessor :source, :column, :format

  def initialize(source, column, format)
    raise "source is required" unless source

    # validate that source "is a" OptimusPrime::Source OR OptimusPrime::Transform
    unless (source.is_a? OptimusPrime::Source or source.is_a? OptimusPrime::Transform)
      raise "source must inherit from either OptimusPrime::Source or OptimusPrime::Transform!"
    end

    @source = source
    @column = column
  end

  def retrieve_data
    index = @source.column_to_index(@column)

    @source.retrieve_data.each do |row|
      # TODO: find efficient way to convert to float (sprintf?)
      new_value = sprintf("#{@format}", row[index])
      row[index] = new_value
    end
  end
end