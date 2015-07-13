require 'spec_helper'

# TODO: replace these tests with Thor-compatible tests
# see https://github.com/backup/backup/blob/master/spec/cli_spec.rb
# and https://github.com/erikhuda/thor/tree/master/spec

root = Pathname.new(__FILE__).parent.parent
ENV['PATH'] = "#{root.join('bin')}:#{ENV['PATH']}"

describe 'optimus' do
  let(:finished) { "Pipeline finished.\n" }
  let(:config_exceptions) { 'spec/supports/config/test-config-exceptions.yml' }
  let(:tmp_destination) { 'tmp/destination.csv' }
  let(:logfile) { 'tmp/optimus.log' }
  let(:output) { File.read(logfile) }
  let(:basepath) { 'spec/supports/config' }
  let(:operate) { 'bundle exec optimus operate' }
  let(:redirect) { " > #{logfile} 2>&1" }
  let(:config_path) { "#{basepath}/test-config.yml" }

  before(:all) { Dir.mkdir('tmp') unless Dir.exist?('tmp') }
  before(:each) { File.delete(logfile) if File.exist?(logfile) }

  describe 'finished output' do
    after(:each) { File.delete(tmp_destination) if File.exist?(tmp_destination) }

    context 'without dependencies' do
      before(:each) { `#{operate} pipeline #{config_path} test_pipeline #{redirect}` }

      it 'prints the finished output when arguments are given' do
        expect(output).to include finished
      end

      it 'writes to the destination csv' do
        expect(File.readlines(tmp_destination).size).to_not eq 0
      end
    end

    context 'with dependencies loading' do
      let(:config_dependencies) { "#{basepath}/test-config-dependencies.yml" }
      context 'command line' do
        it 'requires json and csv' do
          `#{operate} pipeline #{config_path} test_pipeline -d json,csv #{redirect}`
          expect(output).to include('Requiring json', 'Requiring csv')
        end
      end

      context 'yaml config' do
        it 'requires json and csv' do
          `#{operate} pipeline #{config_dependencies} test_pipeline #{redirect}`
          expect(output).to include('Requiring json', 'Requiring csv')
        end
      end

      context 'command line + yaml' do
        it 'requires json, csv and benchmark' do
          `#{operate} pipeline #{config_dependencies} test_pipeline -d benchmark #{redirect}`
          expect(output).to include('Requiring json', 'Requiring csv', 'Requiring benchmark')
        end
      end
    end

    context 'with an exception adapter specified' do
      it 'starts to capture exceptions with Raven' do
        `#{operate} pipeline #{config_exceptions} test_pipeline #{redirect}`
        expect(output).to include('Raven', finished)
      end
    end
  end

  describe 'Help output' do
    it 'prints out help message if no arguments are given' do
      `bundle exec optimus #{redirect}`
      expect(output).to include('Commands:')
    end
  end

  describe 'Missing Pipeline' do
    let(:pipeline_name) { 'inexistent_pipeline' }
    it 'raises a Pipeline not found exception when the specified pipeline is not found' do
      `#{operate} pipeline #{config_path} #{pipeline_name} #{redirect}`
      expect(output).to include("Pipeline #{pipeline_name} does not exist in #{config_path}")
    end
  end

  context 'when running factory' do
    context 'without arguments' do
      it 'displays error and help messages' do
        `#{operate} factory #{redirect}`
        expect(output).to include('ERROR: "optimus factory" was called with no arguments',
                                  'Usage: "optimus factory <file>"')
      end
    end

    context 'with single pipeline' do
      it 'operates correctly' do
        `#{operate} factory #{basepath}/factory-single.yml #{redirect}`
        expect(output).to include('Starting pipeline', 'Pipeline finished', 'Factory finished.')
      end
    end

    context 'with two independent pipelines' do
      it 'runs the second pipeline if the first finished successfully' do
        `#{operate} factory #{basepath}/factory-independent-success.yml #{redirect}`
        expect(output).to include('first finished with exit code 0', 'Starting pipeline second')
      end

      it 'runs the second pipeline even if the first finished unsuccessfully' do
        `#{operate} factory #{basepath}/factory-independent-failure.yml #{redirect}`
        expect(output).to include('first finished with exit code 1',
                                  'Starting pipeline second',
                                  'second finished with exit code 0')
      end
    end

    context 'with one pipeline dependent on another' do
      it 'runs the dependent pipeline if the dpendency finished successfully' do
        `#{operate} factory #{basepath}/factory-dependent-success.yml #{redirect}`
        expect(output).to include('first finished with exit code 0', 'second finished with exit code 0')
      end

      it 'does not run the dependent pipeline if the dpendency finished unsuccessfully' do
        `#{operate} factory #{basepath}/factory-dependent-failure.yml #{redirect}`
        expect(output).to include('first finished with exit code 1', 'Not running second')
      end
    end

    # TODO: test waiting periods when changing to proper Thor tests
  end
end
