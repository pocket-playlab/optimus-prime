require 'spec_helper'
require 'aruba'
require 'aruba/api'
require 'pathname'

include Aruba::Api

root = Pathname.new(__FILE__).parent.parent

# Allows us to run commands directly, without worrying about the CWD
ENV['PATH'] = "#{root.join('bin').to_s}#{File::PATH_SEPARATOR}#{ENV['PATH']}"


module OptimusPrime
  module Sources
    class Test < OptimusPrime::Source
      def initialize(data:, delay: 0)
        @data = data
        @delay = delay
      end

      def each
        sleep @delay
        @data.each { |i| yield i }
      end
    end
  end

  module Destinations
    class Test < OptimusPrime::Destination
      attr_reader :written
      def write(record)
        @received ||= []
        @received << record
      end

      def finish
        @written = @received
      end
    end
  end
end

class IncrementStep < OptimusPrime::Destination
  def write(data)
    sleep 0.1
    push data + 1
  end
end

class DoubleStep < OptimusPrime::Destination
  def write(data)
    push data * 2
  end
end

describe "optimus.rb" do
  let(:help_message) do
    'FUCK'
#<<-eos
#Missing options: file, pipeline
#Usage: optimus.rb --file /path/to/config.yml --p pipeline_identifier
#      -f, --file FILE                  Path to YAML config file
#      -p, --pipeline PIPELINE          Identifier string of pipeline to run
#      -h, --help                       Show this message
#eos
  end

  describe "Help output" do
    it "should print out help message if no arguements are given" do
      #run_simple "optimus.rb"
      run_simple "ls -l"

      expect(all_output).to eq help_message
    end
  end
end