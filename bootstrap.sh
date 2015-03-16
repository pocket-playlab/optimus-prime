#!/bin/bash --login

set -e

# cd straight to /vagrant on login
if ! grep -q 'cd \/vagrant' ~/.bashrc; then
  echo 'cd /vagrant' >> ~/.bashrc
fi
cd /vagrant

# Install packages
sudo apt-add-repository --yes ppa:brightbox/ruby-ng
sudo apt-get update --quiet
sudo -E apt-get install --quiet --assume-yes \
  git libsqlite3-dev ruby2.2 ruby2.2-dev

# Install ruby dependencies
sudo gem install bundler
bundle install

# Run the tests
rspec --format documentation
