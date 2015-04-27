source 'https://rubygems.org'

ruby '2.2.2'

gemspec

group :development, :test do
  gem 'pry-rescue'
  gem 'guard-rspec', require: false
  gem 'pry-byebug'
  gem 'rspec', '~> 3.1.0'
  gem 'mail'
end

group :development do
  gem 'pronto-reek', require: false
  gem 'pronto-rubocop', require: false
  gem 'reek', '2.0.1'  # the lastest version doesn't work with pronto
end

group :test do
  gem 'fakefs', require: 'fakefs/safe'
  gem 'fakes3', github: 'pocket-playlab/fake-s3'
  gem 'vcr'
  gem 'webmock'
  gem 'sqlite3'
  gem 'rspec-mocks'
end
