module OptimusPrime
  class Config
    require 'yaml'

    # parse YAML file and make available as attributes or via getter method
    attr_accessor :file_path, :sources, :transforms, :destinations, :unique_ids

    def initialize(file_path: nil)
      # add error checking to confirm that file_path is indeed a file

      raise 'file not found' unless File.file?(file_path)

      method(__method__).parameters.each do |type, k|
        next unless type == :key
        v = eval(k.to_s)
        instance_variable_set("@#{k}", v) unless v.nil?
      end

      @unique_ids = { 
        source: {}, 
        transform: {}, 
        destination: {}
      }

      @sources = Array.new
      @transforms = Array.new

      self.parse_config
    end

    def parse_config
      yaml = YAML.load_file(self.file_path)

      yaml.each do |settings|
        if settings.has_key?("type")
          formatted = get_format_by_type(settings['type'])
          formatted.each do |key|
            raise "#{key} not found" unless settings.has_key?(key)
          end
          @sources.push settings if settings['type'] == 'source'
          @transforms.push settings if settings['type'] == 'transform'
        else
          raise "yaml doesn't contain type" 
        end
      end
      self.check_for_duplicates(yaml)
    end

    def check_for_duplicates(yaml)
      yaml.each do |item|
        unless identifier_unique?(item['type'], item['unique_identifier'])
          raise "#{item['unique_identifier']} of #{item['type']} duplicate!" 
        end
        self.mark_as_seen(item['type'], item['unique_identifier'])
      end
    end

    def get_sources
      @sources
    end

    def get_source_by_id(unique_id)
      @sources.each do |source|
        return source if source['unique_identifier'] == unique_id
      end
      raise "#{unique_id} not exist"
    end

    def get_transform_by_id(unique_id)
      @transforms.each do |transform|
        return transform if transform['unique_identifier'] == unique_id
      end
      raise "#{unique_id} not exist"
    end

    def mark_as_seen(type, item)
      @unique_ids[type.to_sym][item] = true
    end

    def identifier_unique?(type, id)
      @unique_ids[type.to_sym][id].nil?
    end

    def get_format_by_type(type)
      case type
      when 'source' 
        ['unique_identifier', 'type', 'class', 'columns']
      when 'transform'
        ['unique_identifier', 'type', 'class', 'columns']
      when 'destination'
        ['unique_identifier', 'type', 'class', 'columns']
      else
        raise "type not found"
      end
        
    end

  end
end
