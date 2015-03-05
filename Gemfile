source 'https://rubygems.org'

ruby '2.2.0'

gemspec

group :development, :test do
  gem 'pry-byebug'
  gem 'pronto-rubocop', require: false
end

group :test do
  gem 'fakes3', github: 'pocket-playlab/fake-s3'
  gem 'rspec', '~> 3.1.0'
  gem 'vcr'
  gem 'webmock'
end
