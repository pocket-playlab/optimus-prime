namespace :generate do
  #
  desc 'Generate a timestamped, empty Sequel migration.'
  task :migration, [:name, :fu] do |t, args|
    if args[:name].nil?
      puts 'You must specify a migration name (e.g. rake "generate:migration[create_events]")!'
      exit false
    end

    content = "Sequel.migration do\n  up do\n    \n  end\n\n  down do\n    \n  end\nend\n"
    timestamp = Time.now.to_i
    filename = File.join(File.expand_path('../..', __FILE__), 'optimus_prime', 'persistence', 'migrations', "#{timestamp}_#{args[:name]}.rb")

    File.open(filename, 'w') do |f|
      f.puts content
    end

    puts "Created the migration #{filename}"
  end
end