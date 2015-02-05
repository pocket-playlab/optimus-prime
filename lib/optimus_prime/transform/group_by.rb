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

    @result = {}

    strategies.each do |column, strategy|
      self.send(strategy, [column])
    end

  end

  def check_primary_key(primary_set)
    keys = @source.column_to_index(primary_set)
    grouped_data = @source.retrieve_data.group_by { |arr| arr.values_at(*keys) }
    grouped_data.values.count == @source.retrieve_data.count
  end

  def retrieve_data
    index = @source.column_to_index(@key_columns)

    @source.retrieve_data.each do |row|
      row[index].upcase!
    end
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

  def sum(column)
    index = @source.column_to_index(column).first
    game_total = {}

    @grouped_data.each do |key, value|
      if key.count != 0
        game_total[key] = value.map{ |arr| arr[index].to_f }.inject(:+)
      else
        game_total[['all']] = value.map{ |arr| arr[index].to_f }.inject(:+)
      end
    end

    @result['sum'] = game_total
  end

  def max(column)
    find_max_index = @source.column_to_index(column).first
    max_result = {}

    @grouped_data.each do |key, value|
      if key.count != 0
        max_result[key] = value.max_by{|i| i[find_max_index].to_f}[find_max_index].to_f
      else
        max_result[['all']] = value.max_by{|i| i[find_max_index].to_f}[find_max_index].to_f
      end
    end

    @result['max'] = max_result
  end

  def min(column)
    find_min_index = @source.column_to_index(column).first
    min_result = {}

    @grouped_data.each do |key, value|
      if key.count != 0
        min_result[key] = value.min_by{|i| i[find_min_index].to_f}[find_min_index].to_f
      else
        min_result[['all']] = value.min_by{|i| i[find_min_index].to_f}[find_min_index].to_f
      end
    end

    @result['min'] = min_result
  end

  def median(column)
    find_median_index = @source.column_to_index(column).first
    median_result = {}

    @grouped_data.each do |key, value|
      sorted = value.map{ |arr| arr[find_median_index].to_f }.sort
      length = sorted.length
      if key.count != 0
        median_result[key] = (sorted[(length - 1) / 2] + sorted[length / 2]) / 2.0
      else
        median_result[['all']] = (sorted[(length - 1) / 2] + sorted[length / 2]) / 2.0 
      end
    end

    @result['median'] = median_result
  end

  def mode(column)
    find_mode_index = @source.column_to_index(column).first
    mode_result = {}

    @grouped_data.each do |key, value|
      array_of_number = value.map{ |arr| arr[find_mode_index].to_f }
      freq = array_of_number.inject(Hash.new(0)) { |h,v| h[v] += 1; h }
      if key.count != 0
        mode_result[key] = array_of_number.max_by { |v| freq[v] }
      else
        mode_result[['all']] = array_of_number.max_by { |v| freq[v] }
      end
    end

    @result['mode'] = mode_result
  end

  def average(column)
    find_avg_index = @source.column_to_index(column).first
    average_result = {}

    @grouped_data.each do |key, value|
      array_of_number = value.map{ |arr| arr[find_avg_index].to_f }
      if key.count != 0
        average_result[key] = array_of_number.instance_eval{ reduce(:+) / size }
      else
        average_result[['all']] = array_of_number.instance_eval{ reduce(:+) / size }
      end
    end

    @result['average'] = average_result
  end

  def count(column)
    count_result = {}

    @grouped_data.each do |key, value|
      if key.count != 0
        count_result[key] = value.count
      else
        count_result[['all']] = value.count
      end
    end

    @result['count'] = count_result
  end

  #This method to re-arrange array and groupped
  def group_by
    keys = @source.column_to_index(@key_columns)
    @grouped_data = @source.retrieve_data.group_by { |arr| arr.values_at(*keys) }
  end

  private

  attr_writer :grouped_data

end