# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'rake'
require 'optimus_prime/version'

Gem::Specification.new do |spec|
  spec.name          = 'optimus_prime'
  spec.version       = OptimusPrime::VERSION
  spec.authors       = ['Prair Pusanasurapant', 'M Lertvanasirikul', 'Rick Apichairuk', 'Omar Khan']
  spec.summary       = 'Playlab ETL library'

  spec.files         = FileList["lib/**/*.rb", "bin/*", "[A-Z]*", "test/**/*"].to_a

  spec.executables   = spec.files.grep(/^bin\//) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(/^(test|spec|features)\//)
  spec.require_paths = ['lib']

  spec.add_dependency 'activesupport'
  spec.add_dependency 'aws-sdk',           '~> 2.0.23'
  spec.add_dependency 'google-api-client', '0.8.2'
  spec.add_dependency 'rest-client',       '~> 1.7.2'
  spec.add_dependency 'sequel'
  spec.add_dependency 'yajl-ruby'
  spec.add_dependency 'bigbroda',          '0.0.7'
  spec.add_dependency 'mail'
  spec.add_dependency 'thor'
end
