require 'spec_helper'

# TODO: replace these tests with Thor-compatible tests
# see https://github.com/backup/backup/blob/master/spec/cli_spec.rb
# and https://github.com/erikhuda/thor/tree/master/spec

root = Pathname.new(__FILE__).parent.parent
ENV['PATH'] = "#{root.join('bin')}:#{ENV['PATH']}"

describe 'optimus' do
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

  let(:operate) { 'bundle exec optimus operate' }

  describe 'Finished output' do
    context 'without dependencies' do
      before(:each) do
        @output = `#{operate} pipeline #{config_path} test_pipeline`
      end
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
          @output = `#{operate} pipeline #{config_path} test_pipeline -d json,csv`
          expect(@output).to include 'Requiring json'
          expect(@output).to include 'Requiring csv'
        end
      end

      context 'yaml config' do
        it 'should require json and csv' do
          @output = `#{operate} pipeline #{config_dependencies} test_pipeline`
          expect(@output).to include 'Requiring json'
          expect(@output).to include 'Requiring csv'
        end
      end

      context 'command line + yaml' do
        it 'should require json, csv and benchmark' do
          @output = `#{operate} pipeline #{config_dependencies} test_pipeline -d benchmark`
          expect(@output).to include 'Requiring json'
          expect(@output).to include 'Requiring csv'
          expect(@output).to include 'Requiring benchmark'
        end
      end
    end
  end

  describe 'Help output' do
    it 'should print out help message if no arguments are given' do
      output = `bundle exec optimus`
      expect(output).to include('Commands:')
    end
  end

  describe 'Missing Pipeline' do
    let(:pipeline_name) { 'inexistent_pipeline' }
    it 'should raise a Pipeline not found exception when the specified pipeline is not found' do
      output = `#{operate} pipeline #{config_path} #{pipeline_name} 2>&1`
      expect(output).to include("Pipeline #{pipeline_name} does not exist in #{config_path}")
    end
  end

  context 'when running factory' do
    let(:factory_log) { '/tmp/factory.log' }
    before { File.delete(factory_log) if File.exist?(factory_log) }

    def execute(command)
      `#{command} > #{factory_log} 2>&1`
    end

    def output
      File.read(factory_log)
    end

    context 'without arguments' do
      it 'displays error and help messages' do
        execute("#{operate} factory")
        expect(output).to include('ERROR: "optimus factory" was called with no arguments',
                                  'Usage: "optimus factory <file>"')
      end
    end

    context 'with single pipeline' do
      let(:factory_single) { 'spec/supports/config/factory-single.yml' }
      it 'operates correctly' do
        execute("#{operate} factory #{factory_single}")
        expect(output).to include('Starting pipeline', 'Pipeline finished', 'Factory finished.')
      end
    end

    context 'with two independent pipelines' do
      let(:independent_success) { 'spec/supports/config/factory-independent-success.yml' }
      it 'runs the second pipeline if the first finished successfully' do
        execute("#{operate} factory #{independent_success}")
        expect(output).to include('first finished with exit code 0',
                                  'Starting pipeline second')
      end

      let(:independent_failure) { 'spec/supports/config/factory-independent-failure.yml' }
      it 'runs the second pipeline even if the first finished unsuccessfully' do
        execute("#{operate} factory #{independent_failure}")
        expect(output).to include('first finished with exit code 1',
                                  'Starting pipeline second',
                                  'second finished with exit code 0')
      end
    end

    context 'with one pipeline dependent on another' do
      let(:dependent_success) { 'spec/supports/config/factory-dependent-success.yml' }
      it 'runs the dependent pipeline if the dpendency finished successfully' do
        execute("#{operate} factory #{dependent_success}")
        expect(output).to include('first finished with exit code 0',
                                  'second finished with exit code 0')
      end

      let(:dependent_failure) { 'spec/supports/config/factory-dependent-failure.yml' }
      it 'does not run the dependent pipeline if the dpendency finished unsuccessfully' do
        execute("#{operate} factory #{dependent_failure}")
        expect(output).to include('first finished with exit code 1',
                                  'Not running second')
      end
    end

    # TODO: test waiting periods when changing to proper Thor tests
  end
end
