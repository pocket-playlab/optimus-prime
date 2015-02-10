require_relative '../optimus_init.rb'

class UpperCase < OptimusPrime::Transform

  attr_accessor :source, :column

  def initialize(source, column)
    raise "source is required" unless source

    # validate that source "is a" OptimusPrime::Source OR OptimusPrime::Transform
    unless (source.is_a? OptimusPrime::Source or source.is_a? OptimusPrime::Transform)
      raise "source must inherit from either OptimusPrime::Source or OptimusPrime::Transform!"
    end

    @source = source
    @column = column
  end

  def get_data
    index = @source.column_to_index(@column)

    @source.get_data.each do |row|
      row[index].upcase!
    end
  end
end