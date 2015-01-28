module OptimusPrime
  class Config
    require 'yaml'

    # parse YAML file and make available as attributes or via getter method
    attr_accessor :file_path
   
    def initialize(file_path: nil)
      # add error checking to confirm that file_path is indeed a file

      method(__method__).parameters.each do |type, k|
        next unless type == :key
        v = eval(k.to_s)
        instance_variable_set("@#{k}", v) unless v.nil?
      end
    end

    def parse_config
      puts 'HELLO!'
      puts self.file_path

      yaml = YAML.load_file(self.file_path)

      puts yaml.inspect
    end

    def get_data
    end 

  end
end
