module OptimusPrime
  module Transformers
    # The CsvParser transformer parse value that have csv format
    #
    # Params;
    #     header: is csv already have header or not
    #     columns: array of header in csv file (set it's as empty if csv already have header)
    #
    # Example # 1
    # input : 'id,name'
    #         '1,Alice'
    #         '2,Bob'
    # initialize : header: true, columns: []
    # output : { {'id' => '1', 'name' => 'Alice'},{'id' => '2', 'name' => 'Bob'}  }

    # Example # 2
    # input : '22,Alice'
    #         '23,Bob'
    # initialize : header: false, columns: ['age','name']
    # output : { {'age' => '22', 'name' => 'Alice'},{'age' => '23', 'name' => 'Bob'}  }

    class CsvParser < Destination
      def initialize(header: true, columns: [])
        raise 'Please specify columns' unless header || columns.any?
        @header = header
        @columns = columns
      end

      def write(csv_content)
        CSV.parse csv_content, headers: @header do |row|
          if @header
            push row.to_h
          else
            hash = {}
            @columns.each_with_index { |h, i| hash[h] = row[i] }
            push hash
          end
        end
      end
    end
  end
end
