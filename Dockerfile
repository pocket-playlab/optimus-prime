FROM ruby:2.2.2-slim

# Throw errors if Gemfile has been modified since Gemfile.lock
RUN bundle config --global frozen 1

# The following packages are installed:
# - build-essential is required to build native extensions such as 'json'
# - cmake and pkg-config are required for pronto gems
# - git for installing gems from github
# - libsqlite3-dev for sqlite gem, needed to run the tests
# We also remove lists in order to cache this step
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
 	build-essential \
	cmake \
	pkg-config \
	git \
	libsqlite3-dev  \
&& rm -rf /var/lib/apt/lists/*

WORKDIR .

# Install gems
# For now CircleCI only supports Docker 1.4, for that version
# relative paths on ADD/COPY are not allowed. The following PR
# added that feature to 1.5 version:
# https://github.com/docker/docker/pull/9635
# In the meantime we have to stick with absolute paths.
COPY Gemfile Gemfile
COPY optimus_prime.gemspec optimus_prime.gemspec

# The following dependency is mandatory before the bundle install
RUN mkdir -p lib/optimus_prime
COPY lib/optimus_prime/version.rb lib/optimus_prime/version.rb

# RUN gem install bundler
RUN bundle install

# Copy application source. We do this after installing the gems so that docker
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY . /usr/src/app

# For now by default we just run the optimus script with no argument
ENTRYPOINT [ "bundle", "exec" ]
CMD [ "bin/optimus" ]