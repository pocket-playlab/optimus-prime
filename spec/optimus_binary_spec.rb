require 'spec_helper'

root = Pathname.new(__FILE__).parent.parent
ENV['PATH'] = "#{root.join('bin')}:#{ENV['PATH']}"

describe 'optimus.rb' do
  let(:finished) do
    <<-eos
Pipeline finished.
    eos
  end

  let(:config_path) { 'spec/supports/config/test-config.yml' }
  let(:config_dependencies) { 'spec/supports/config/test-config-dependencies.yml' }

  def tmp_destination
    @tmp_destination ||= 'tmp/destination.csv'
  end

  def delete_destination
    File.delete(tmp_destination) if File.exist?(tmp_destination)
  end

  before(:all) { Dir.mkdir('tmp') unless Dir.exist?('tmp') }

  describe 'Finished output' do

    context 'without dependencies' do
      before(:each) { @output = `bundle exec optimus.rb -p test_pipeline -f #{config_path}` }
      after(:each) { delete_destination }

      it 'should print out the finished output when arguments are given ' do
        expect(@output).to eq finished
      end

      it 'should write in the destination csv' do
        destination = File.open(tmp_destination, 'r')
        expect(destination.readlines.size).to_not eq 0
        destination.close
      end
    end

    context 'with dependencies loading' do

      after(:each) { delete_destination }

      context 'command line' do

        it 'should require json and csv' do
          @output = `bundle exec optimus.rb -p test_pipeline -f #{config_path} -d json,csv`
          expect(@output).to include 'Requiring json'
          expect(@output).to include 'Requiring csv'
        end

      end

      context 'yaml config' do
        it 'should require json and csv' do
          @output = `bundle exec optimus.rb -p test_pipeline -f #{config_dependencies}`
          expect(@output).to include 'Requiring json'
          expect(@output).to include 'Requiring csv'
        end
      end

      context 'command line + yaml' do

        it 'should require json, csv and benchmark' do
          @output = `bundle exec optimus.rb -p test_pipeline -f #{config_dependencies} -d benchmark`
          expect(@output).to include 'Requiring json'
          expect(@output).to include 'Requiring csv'
          expect(@output).to include 'Requiring benchmark'
        end

      end

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
