guard :rspec, cmd: 'bundle exec rescue rspec' do
  watch(%r{^spec/(.*)_spec\.rb$})
  watch(%r{^lib/(.*)\.rb$}) { |m| "spec/#{m[1]}_spec.rb" }
  watch('spec/spec_helper.rb')  { 'spec' }
  watch('bin/optimus') { 'spec/optimus_binary_spec.rb' }
end
