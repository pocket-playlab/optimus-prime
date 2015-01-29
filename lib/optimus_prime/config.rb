module OptimusPrime
  class Config
    require 'yaml'

    # parse YAML file and make available as attributes or via getter method
    attr_accessor :file_path, :sources, :transforms, :destinations, :unique_ids

    def initialize(file_path: nil)
      # add error checking to confirm that file_path is indeed a file

      method(__method__).parameters.each do |type, k|
        next unless type == :key
        v = eval(k.to_s)
        instance_variable_set("@#{k}", v) unless v.nil?
      end

      @unique_ids = { 
        sources: {}, 
        transforms: {}, 
        destinations: {}
      }

      parse_config
    end

    def parse_config
      yaml = YAML.load_file(self.file_path)

      ['sources', 'transforms', 'destinations'].each do |type|
        check_for_duplicates(type, yaml)
      end

      @sources = yaml['sources']
      @transforms = yaml['transforms']
      @destinations = yaml['destinations']
    end

    def check_for_duplicates(type, yaml)
      if yaml[type].is_a? Array
        yaml[type].each do |item|
          raise "#{type} duplicate!" unless is_identifier_unique?(type, item['source']['unique_identifier'])
          self.mark_as_seen(type, item)
        end
      end
    end

    def get_sources
      @sources
    end

    def get_source_by_id(unique_id)
      @sources.each do |source|
        return source if source['unique_identifier'] == unique_id
      end
    end

    def mark_as_seen(type, item)
      @unique_ids[type.to_sym][item['source']['unique_identifier']] = true
    end

    def is_identifier_unique?(type, id)
      @unique_ids[type.to_sym][id].nil?
    end

  end
end
