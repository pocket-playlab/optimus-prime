source 'https://rubygems.org'

ruby '2.2.0'

gemspec

group :development, :test do
  gem 'pronto-reek', require: false
  gem 'awesome_print'
  gem 'pronto-rubocop', require: false
  gem 'pry-byebug'
  gem 'rspec', '~> 3.1.0'
end

group :test do
  gem 'fakefs', require: 'fakefs/safe'
  gem 'fakes3', github: 'pocket-playlab/fake-s3'
  gem 'vcr'
  gem 'webmock'
end
