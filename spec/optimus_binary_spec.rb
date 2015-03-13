require 'spec_helper'

root = Pathname.new(__FILE__).parent.parent
ENV['PATH'] = "#{root.join('bin')}#{File::PATH_SEPARATOR}#{ENV['PATH']}"

describe 'optimus.rb' do
  let(:finished) do
    <<-eos
Pipeline finished.
    eos
  end

  let(:config_path) { 'spec/supports/config/test-config.yml' }

  def tmp_destination
    @tmp_destination ||= 'tmp/spec/destination.csv'
  end

  def delete_destination
    File.delete(tmp_destination) if File.exist?(tmp_destination)
  end

  after(:each) do
    delete_destination
  end

  describe 'Finished output' do
    before(:each) do
      @output = `optimus.rb -p test_pipeline -f #{config_path}`
    end

    it 'should print out the finished output when arguments are given ' do
      expect(@output).to eq finished
    end

    it 'should write in the destination csv' do
      destination = File.open(tmp_destination, 'r')
      expect(destination.readlines.size).to_not eq 0
      destination.close
    end
  end

  describe 'Help output' do
    it 'should print out help message if no arguments are given' do
      output = `optimus.rb`
      expect(output).to include('Missing options')
    end
  end

  describe 'Missing Pipeline' do
    it 'should raise a Pipeline not found exception when the specified pipeline is not found' do
      output = `optimus.rb  -p inexistent_pipeline -f #{config_path} 2>&1`
      expect(output).to include('Pipeline not found (RuntimeError)')
    end
  end
end
