source 'https://rubygems.org'

ruby '2.2.0'

gemspec

group :development, :test do
  gem 'pry-byebug'
  gem 'pronto-rubocop', require: false
  gem 'pronto-reek', require: false
  gem 'awesome_print'
end

group :test do
  gem 'fakefs', require: 'fakefs/safe'
  gem 'fakes3', github: 'pocket-playlab/fake-s3'
  gem 'rspec', '~> 3.1.0'
  gem 'vcr'
  gem 'webmock'
  gem 'aruba'
end
