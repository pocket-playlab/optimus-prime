require_relative '../optimus_init.rb'

class Join < OptimusPrime::Transform

    attr_accessor :source_a, :col_a, :source_b, :col_b, :method

    def initialize(source_a, col_a, source_b, col_b, method)
      # allow inner, outer
      method ||= 'inner'
      @method = method

      raise "source_a is required" unless source_a
      raise "col_a is required" unless col_a 
      raise "source_b is required" unless source_b
      raise "col_b is required" unless col_b 

      unless (source_a.is_a? OptimusPrime::Source or source_a.is_a? OptimusPrime::Transform)
        raise "source_a must inherit from either OptimusPrime::Source or OptimusPrime::Transform!"
      end

      unless (source_b.is_a? OptimusPrime::Source or source_b.is_a? OptimusPrime::Transform)
        raise "source_a must inherit from either OptimusPrime::Source or OptimusPrime::Transform!"
      end

      @source_a = source_a
      @source_b = source_b

      # check that col_a is indeed a column of source_a
      # check that col_b is indeed a column of source_b

      @col_a = col_a
      @col_b = col_b
    end

    def get_data

      set_a = @source_a.get_data
      set_b = @source_b.get_data

      index = {}

      set_b_key_index = source_b.column_to_index(@col_b)
      set_b_row_index = 0

      set_b.each do |row|
        index[row[set_b_key_index]] = set_b_row_index
        set_b_row_index++
      end
      
      set_a_key_index = source_a.column_to_index(@col_a)
      set_b_row_index = 0

      set_a.each do |row|

      end

      @source_a
      @col_a

      @source_b
      @col_b

    end
  end
end