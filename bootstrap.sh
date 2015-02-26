#!/bin/bash --login

set -e

# cd straight to /vagrant on login
if ! grep -q 'cd \/vagrant' ~/.bashrc; then
  echo 'cd /vagrant' >> ~/.bashrc
fi
cd /vagrant

# Don't prompt for mysql root password
export DEBIAN_FRONTEND=noninteractive

# Install packages
sudo apt-get update --quiet
sudo -E apt-get install --quiet --assume-yes curl git postgresql mysql-server libpq-dev libmysqlclient-dev

# Install rvm and ruby
if ! rvm use ruby-2.2.0; then
  # Install mpapis public key
  gpg --keyserver hkp://keys.gnupg.net --recv-keys D39DC0E3

  curl -sSL https://get.rvm.io | bash -s stable
  source ~/.rvm/scripts/rvm
  rvm install --quiet-curl ruby-2.2.0
  rvm use ruby-2.2.0
fi

# Install ruby dependencies
bundle install

# Set mysql root password
mysqladmin -u root password root || echo 'mysql password already set'

# Set postgres user password
echo "ALTER USER postgres WITH password 'root';" |
  sudo -u postgres psql || echo 'postgres password already set'

# Configure mysql to use utf-8
sudo tee /etc/mysql/conf.d/mysqld_unicode.cnf << 'EOF'
[client]
default-character-set = utf8

[mysql]
default-character-set = utf8

[mysqld]
collation-server = utf8_unicode_ci
character-set-server = utf8
init-connect = 'SET NAMES utf8'
EOF

# Allow postgres login with password
sudo tee /etc/postgresql/9.3/main/pg_hba.conf << 'EOF'
local   all             all                                     md5
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5
EOF

# Restart databases
sudo service mysql restart
sudo service postgresql restart
