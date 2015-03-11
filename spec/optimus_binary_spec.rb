require 'spec_helper'

describe 'optimus.rb' do
  let(:finished) do
    <<-eos
Pipeline finished.
    eos
  end

  let(:config_path) { 'spec/supports/config/test-config.yml' }

  def truncate_destination
    File.open('spec/supports/csv/destination.csv', 'w') { |file| file.truncate(0) }
  end

  before(:all) do
    truncate_destination
  end

  after(:each) do
    truncate_destination
  end

  describe 'Finished output' do
    before(:each) do
      @output = `bundle exec optimus.rb -p test_pipeline -f #{config_path}`
    end

    it 'should print out the finished output when arguments are given ' do
      expect(@output).to eq finished
    end

    it 'should write in the destination csv' do
      destination = File.open('spec/supports/csv/destination.csv', 'r')
      expect(destination.readlines.size).to_not eq 0
    end
  end

  describe 'Help output' do
    it 'should print out help message if no arguments are given' do
      output = `bundle exec optimus.rb`
      expect(output).to include('Missing options')
    end
  end

  describe 'Missing Pipeline' do
    it 'should raise a Pipeline not found exception when the specified pipeline is not found' do
      output = `bundle exec optimus.rb  -p inexistent_pipeline -f #{config_path} 2>&1`
      expect(output).to include('Pipeline not found (RuntimeError)')
    end
  end
end
