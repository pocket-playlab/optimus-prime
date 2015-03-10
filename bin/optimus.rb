#!/usr/bin/env ruby

require 'yaml'
require 'awesome_print'
require 'optparse'
require 'optimus_prime'
require 'pry'

$options = {}

optparse = OptionParser.new do |opts|
  opts.banner = 'Usage: optimus.rb --file /path/to/config.yml --pipeline pipeline_identifier'

  opts.on_tail('-h', '--help', 'Show this message') do
    puts opts
    exit
  end

  opts.on('-f', '--file FILE', String, 'Path to YAML config file') do |file|
    $options[:file] = file
  end

  opts.on('-p', '--pipeline PIPELINE', String, 'Identifier string of pipeline to run') do |pipeline|
    $options[:pipeline] = pipeline
  end
end

begin
  optparse.parse!
  mandatory = [:file, :pipeline]                                   # Enforce the presence of
  missing = mandatory.select { |param| $options[param].nil? }       # the -f and -p switches
  unless missing.empty?                                            #
    puts "Missing options: #{missing.join(', ')}"                  #
    puts optparse                                                  #
    exit                                                           #
  end                                                              #
rescue OptionParser::InvalidOption, OptionParser::MissingArgument  #
  puts $ERROR_INFO.to_s                                                     # Friendly output when parsing fails
  puts optparse                                                    #
  exit                                                             #
end

def get_graph
  config = YAML.load_file($options[:file])
  raw_config = config[$options[:pipeline]]['graph']

  symbolize_keys = lambda do |h|
    Hash === h ?
        Hash[
            h.map do |k, v|
              [k.respond_to?(:to_sym) ? k.to_sym : k, symbolize_keys[v]]
            end
        ] : h
  end

  symbolize_keys[raw_config]
end

graph = get_graph
pipeline = OptimusPrime::Pipeline.new(graph)

pipeline.start
pipeline.wait

pp graph

pipeline.finished? ? puts('Pipeline finished.') : raise('Pipeline failed to finish!')
