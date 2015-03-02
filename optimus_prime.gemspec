# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'optimus_prime/version'

Gem::Specification.new do |spec|
  spec.name          = 'optimus_prime'
  spec.version       = OptimusPrime::VERSION
  spec.authors       = ['Prair Pusanasurapant', 'M Lertvanasirikul', 'Rick Apichairuk', 'Omar Khan']
  spec.summary       = 'Playlab ETL library'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(/^bin\//) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(/^(test|spec|features)\//)
  spec.require_paths = ['lib']

  spec.add_dependency 'aws-sdk',     '~> 2.0.23'
  spec.add_dependency 'rest-client', '~> 1.7.2'
  spec.add_dependency 'yajl-ruby'
  spec.add_dependency 'sqlite3'
  spec.add_dependency 'sequel'
end
