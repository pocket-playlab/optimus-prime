FROM ruby:2.2.1

# Throw errors if Gemfile has been modified since Gemfile.lock
RUN bundle config --global frozen 1

# The following packages are installed:
# - cmake and pkg-config are required for pronto gems
# - git for installing gems from github
# - libsqlite3-dev for sqlite gem, needed to run the tests
# We also remove lists in order to cache this step
RUN apt-get update && apt-get install -y cmake pkg-config git libsqlite3-dev --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Run as a normal user. We need to set uid to 1000 so that
# this user can write to the host volume when using
# boot2docker in development:
# https://github.com/boot2docker/boot2docker/issues/581
RUN adduser --system --uid 1000 playlab
USER playlab

# Create application directory
ENV HOME /home/playlab
ENV ROOT ${HOME}/app
RUN mkdir ${ROOT}
WORKDIR ${ROOT}

# Install gems
ENV BUNDLE_APP_CONFIG ${ROOT}/.bundle
ENV GEM_HOME ${HOME}/ruby/2.2.1
COPY Gemfile Gemfile
COPY optimus_prime.gemspec optimus_prime.gemspec

# The following dependency is mandatory before the bundle install
RUN mkdir -p ${ROOT}/lib/optimus_prime
COPY lib/optimus_prime/version.rb lib/optimus_prime/version.rb

RUN gem install bundler
RUN bundle install --path ${HOME}

# Copy application source. We do this after installing the gems so that docker
# can cache the bundle install step.
COPY . ${ROOT}

# For now by default we just run the optimus script with no argument
ENTRYPOINT [ "bundle", "exec" ]
CMD [ "bin/optimus" ]