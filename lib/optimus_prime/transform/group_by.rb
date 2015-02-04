require_relative '../../optimus_init.rb'

class GroupBy < OptimusPrime::Transform

  attr_accessor :source, :key_columns, :strategies, :result

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

    # validate that source "is a" OptimusPrime::Source OR OptimusPrime::Transform
    unless (source.is_a? OptimusPrime::Source or source.is_a? OptimusPrime::Transform)
      raise "source must inherit from either OptimusPrime::Source or OptimusPrime::Transform!"
    end

    @source = source

    # EM TODO:
    # key_columns should be an array that defines the "unique key for a row"
    # validate this

    raise "key_columns should be an array" unless key_columns.is_a? Array

    keys = source.column_to_index(key_columns)
    raise "#{key_columns} are not unique key" if source.retrieve_data.group_by { |arr| arr.values_at(*keys) }.count != source.retrieve_data.count
    @key_columns = keys

    # EM TODO:
    # strategies should be a hash where keys are column names and values are the strategies to use
    # for the group by

    raise "strategies must be hash" unless strategies.is_a? Hash

    strategies.values.each do |value|
      raise "#{value} not include in strategies" unless operations.include? value
    end

    strategies.keys.each do |key|
      raise "#{key} is not column name in Source" unless source.columns
    end
    
    @strategies = strategies
    # validate that strategies is a hash
    # validate that keys of strategies are valid column names in source
    # validate that values of strajtegies are valid strategies
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
    col_index = @source.column_to_index(@strategies.keys)
    index = col_index.first

    data = @source.retrieve_data

    @result = data.max_by{|i| i[index].to_f}[@key_columns.first]
  end

  def min
    find_min_index = @source.column_to_index(@strategies.keys).first

    data = @source.retrieve_data
    @result = data.min_by{|i| i[find_min_index].to_f}[@key_columns.first]
  end

end