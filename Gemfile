source 'https://rubygems.org'

ruby '2.2.0'

gemspec

group :development, :test do
  gem 'pry-byebug'
  gem 'rspec', '~> 3.1.0'
  gem 'reek', '2.0.1'  # the lastest version doesn't work with pronto
end

group :development do
  gem 'pronto-reek', require: false
  gem 'pronto-rubocop', require: false
end

group :test do
  gem 'fakefs', require: 'fakefs/safe'
  gem 'fakes3', github: 'pocket-playlab/fake-s3'
  gem 'vcr'
  gem 'webmock'
end
