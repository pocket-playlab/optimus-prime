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
packages=(
  ruby2.2 ruby2.2-dev  # Latest ruby from brightbox ppa
  cmake pkg-config     # Needed for pronto gems
  git                  # For installing gems from github
  libsqlite3-dev       # For sqlite gem, needed to run the tests
)
sudo -E apt-get install --quiet --assume-yes ${packages[@]}

# Install ruby dependencies
sudo gem install bundler
bundle install

# Run the tests
rspec --format documentation
