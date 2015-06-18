#export DATABASE_URL=postgres://thibault:fu@localhost:5432/megatron
namespace :db do
  require "sequel"
  Sequel.extension :migration
  DB = Sequel.connect(ENV['DATABASE_URL'])

  desc "Prints current schema version"
  task :version do
    version = if DB.tables.include?(:schema_migrations)
      DB[:schema_migrations].to_a.last[:filename]
    end || 0

    puts "Last Migration: #{version}"
  end

  desc "Perform migration up to latest migration available"
  task :migrate do
    migrations_path = File.join(File.expand_path('../..', __FILE__), 'optimus_prime', 'persistence', 'migrations')
    Sequel::Migrator.run(DB, migrations_path)
    Rake::Task['db:version'].execute
  end

  desc "Perform rollback to specified target or full rollback as default"
  task :rollback, :target do |t, args|
    args.with_defaults(:target => 0)

    migrations_path = File.join(File.expand_path('../..', __FILE__), 'optimus_prime', 'persistence', 'migrations')
    Sequel::Migrator.run(DB, migrations_path, :target => args[:target].to_i)
    Rake::Task['db:version'].execute
  end

  desc "Perform migration reset (full rollback and migration)"
  task :reset do
    migrations_path = File.join(File.expand_path('../..', __FILE__), 'optimus_prime', 'persistence', 'migrations')
    Sequel::Migrator.run(DB, migrations_path, :target => 0)
    Sequel::Migrator.run(DB, migrations_path)
    Rake::Task['db:version'].execute
  end
end