require_relative '../../optimus_init.rb'

class GroupBy < OptimusPrime::Transform

  attr_accessor :source, :key_columns, :strategies, :result
  attr_reader :grouped_data

  # notes: perhaps strategy should not be global and instead by a column-by-column specified strategy
  # with a default of last seen value.
  # other strategies might include:
  #   sum - just like sql SUM and group by  
  #   max - choose the maximum value
  #   min - choose the min value
  #   median
  #   mode
  #   average
  #   count
  #   first - just take first seen value
  #   last - just take last seen value
  def initialize(source, key_columns, strategies)
    raise "source is required" unless source

    unless (source.is_a? OptimusPrime::Source or source.is_a? OptimusPrime::Transform)
      raise "source must inherit from either OptimusPrime::Source or OptimusPrime::Transform!"
    end

    @source = source

    raise "key_columns should be an array" unless key_columns.is_a? Array

    @key_columns = key_columns

    raise "strategies must be hash" unless strategies.is_a? Hash

    strategies.values.each do |value|
      raise "#{value} not include in strategies" unless operations.include? value
    end

    strategies.keys.each do |key|
      raise "#{key} is not column name in Source" unless source.columns.include? key
    end
    
    @strategies = strategies

    self.group_by
    
  end


  def retrieve_data
    index = @source.column_to_index(@key_columns)

    @source.retrieve_data.each do |row|
      row[index].upcase!
    end
  end

  def result
    @result
  end

  def operations
    ['sum','max','min','median','mode','average','count','first','last']
  end


  # NOTE: This is for collapsing all columns, must adjust to do this per column
  # just take first record and delete all other rows with duplicate keys
  def collapse_on_first
    index = @source.column_to_index(@key_columns)

    # for keeping track of order of rows
    order = []

    # for keeping track of data and making sure that keys are unique
    unique_rows_by_key = {}

    @source.retrieve_data.each do |row|
      key = row[index]

      unless unique_rows_by_key.has_key?(key)
        already_have[key] = row

        order.push(key)
      end
    end

    new_set = []

    # in the order the rows appeared...
    order.each do |key|
      # insert them back into the new data set
      new_set.push unique_rows_by_key[key]
    end

    return new_set
  end


  # NOTE: This is for collapsing all columns, must adjust to do this per column
  # just take last record and delete all other rows with duplicate keys
  def collapse_on_last
    index = @source.column_to_index(@key_columns)

    # for keeping track of order of rows
    order = []

    # for keeping track of data and making sure that keys are unique
    unique_rows_by_key = {}

    @source.retrieve_data.reverse_each do |row|
      key = row[index]

      unless unique_rows_by_key.has_key?(key)
        already_have[key] = row

        order.push(key)
      end
    end

    new_set = []

    # in the order the rows appeared...
    order.each do |key|
      # insert them back into the new data set
      new_set.push unique_rows_by_key[key]
    end

    return new_set
  end

  def sum
    col_index = @source.column_to_index(@strategies.keys)
    index = col_index.first

    data = @source.retrieve_data

    @result = data.map{ |arr| arr[index].to_f }.inject(:+)
  end

  def max
    find_max_index = @source.column_to_index(@strategies.keys).first

    data = @source.retrieve_data
    @result = data.max_by{|i| i[find_max_index].to_f}
  end

  def min
    find_min_index = @source.column_to_index(@strategies.keys).first

    data = @source.retrieve_data
    @result = data.min_by{|i| i[find_min_index].to_f}
  end

  def median
    find_median_index = @source.column_to_index(@strategies.keys).first

    data = @source.retrieve_data
    sorted = data.map{ |arr| arr[find_median_index].to_f }.sort
    length = sorted.length
    
    @result = (sorted[(length - 1) / 2] + sorted[length / 2]) / 2.0 
  end

  def mode
    find_mode_index = @source.column_to_index(@strategies.keys).first

    data = @source.retrieve_data
    array_of_number = data.map{ |arr| arr[find_mode_index].to_f }
    freq = array_of_number.inject(Hash.new(0)) { |h,v| h[v] += 1; h }
    @result = array_of_number.max_by { |v| freq[v] }
  end

  def average
    find_avg_index = @source.column_to_index(@strategies.keys).first

    data = @source.retrieve_data
    array_of_number = data.map{ |arr| arr[find_avg_index].to_f }

    @result = array_of_number.instance_eval{ reduce(:+) / size }
  end

  def count
    @result = @source.retrieve_data.count
  end

  #This method to re-arrange array and groupped
  def group_by
    keys = @source.column_to_index(@key_columns)
    @grouped_data = @source.retrieve_data.group_by { |arr| arr.values_at(*keys) }
  end

  private

  attr_writer :grouped_data

end