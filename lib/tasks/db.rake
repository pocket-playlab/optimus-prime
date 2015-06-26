namespace :db do
  require 'sequel'
  Sequel.extension :migration

  def migrations_path
    File.join(File.expand_path('../../..', __FILE__), 'migrations')
  end

  def db
    @db ||= Sequel.connect(ENV['PERSISTENCE_DATABASE_URL'])
  end

  desc 'Prints current schema version'
  task :version do
    version = if db.tables.include?(:schema_migrations)
                db[:schema_migrations].to_a.last[:filename] if db[:schema_migrations].to_a.any?
              end || 0

    puts "Last Migration: #{version}"
  end

  desc 'Perform migration up to latest migration available'
  task :migrate do
    Sequel::Migrator.run(db, migrations_path)
    Rake::Task['db:version'].execute
  end

  desc 'Perform rollback to specified target or full rollback as default'
  task :rollback, :target do |t, args|
    args.with_defaults(target: 0)

    Sequel::Migrator.run(db, migrations_path, target: args[:target].to_i)
    Rake::Task['db:version'].execute
  end

  desc 'Perform migration reset (full rollback and migration)'
  task :reset do
    Sequel::Migrator.run(db, migrations_path, target: 0)
    Sequel::Migrator.run(db, migrations_path)
    Rake::Task['db:version'].execute
  end
end
