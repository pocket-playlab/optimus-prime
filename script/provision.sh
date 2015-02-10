#!/bin/bash --login

set -e

# Install packages
sudo apt-get update --quiet
sudo -E apt-get install --quiet --assume-yes curl git libpq-dev libmysqlclient-dev

# Install rvm and ruby
if ! rvm use ruby-2.2.0; then
  # Install mpapis public key
  gpg --keyserver hkp://keys.gnupg.net --recv-keys D39DC0E3

  curl -sSL https://get.rvm.io | bash -s stable
  source ~/.rvm/scripts/rvm
  rvm install --quiet-curl ruby-2.2.0
  rvm use ruby-2.2.0
fi

# cd straight to /vagrant on login
if ! grep -q 'cd \/vagrant' ~/.bashrc; then
  echo 'cd /vagrant' >> ~/.bashrc
fi
cd /vagrant

# Install ruby dependencies
bundle install
