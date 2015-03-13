#!/usr/bin/env ruby

# Example:
# bundle exec optimus.rb -f spec/supports/config/test-config.yml -p test_pipeline

require 'yaml'
require 'optparse'
require 'erb'
require 'optimus_prime'

options = {}

optparse = OptionParser.new do |opts|
  opts.banner = 'Usage: optimus.rb --file /path/to/config.yml --pipeline pipeline_identifier'

  opts.on_tail('-h', '--help', 'Show this message') do
    puts opts
    exit
  end

  opts.on('-f', '--file FILE', String, 'Path to YAML config file') do |file|
    options[:file] = file
  end

  opts.on('-p', '--pipeline PIPELINE', String, 'Identifier string of pipeline to run') do |pipeline|
    options[:pipeline] = pipeline
  end
end

begin
  optparse.parse!
  mandatory = [:file, :pipeline]                                   # Enforce the presence of
  missing = mandatory.select { |param| options[param].nil? }       # the -f and -p switches
  unless missing.empty?                                            #
    puts "Missing options: #{missing.join(', ')}"                  #
    puts optparse                                                  #
    exit                                                           #
  end                                                              #
rescue OptionParser::InvalidOption, OptionParser::MissingArgument  #
  puts $ERROR_INFO.to_s                                            # Friendly output when parsing
  puts optparse                                                    # fails
  exit                                                             #
end

def load_yaml(options)
  config = YAML.load(ERB.new(File.read(options[:file])).result)
  raise 'Pipeline not found' unless config[options[:pipeline]]
  config
end

def symbolize_nested_keys(h)
  if h.is_a? Hash
    Hash[h.map { |k, v| [k.respond_to?(:to_sym) ? k.to_sym : k, symbolize_nested_keys(v)] }]
  else
    h
  end
end

def load_graph(options)
  config = load_yaml(options)
  raw_config = config[options[:pipeline]]['graph']
  symbolize_nested_keys(raw_config)
end

graph = load_graph(options)

pipeline = OptimusPrime::Pipeline.new(graph)
pipeline.start
pipeline.wait

pipeline.finished? ? puts('Pipeline finished.') : raise('Pipeline failed to finish!')
