this_dir_path = File.join(File.dirname(__FILE__))
lib_dir_path = File.join(this_dir_path, "lib")
conf_dir_path = File.join(this_dir_path, "conf")
conf_file_path = File.join(conf_dir_path, "optimus_prime.yaml")

$LOAD_PATH.unshift(lib_dir_path)

require 'optimus_prime'

loader = OptimusPrime::Loader.new

puts loader

# parse config file

config = OptimusPrime::Config.new(file_path: conf_file_path)

config.parse_config
