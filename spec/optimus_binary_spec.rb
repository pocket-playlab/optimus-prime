require 'spec_helper'
require 'aruba'
require 'aruba/api'

root = Pathname.new(__FILE__).parent.parent

# Allows us to run commands directly, without worrying about the CWD
ENV['PATH'] = "#{root.join('bin')}#{File::PATH_SEPARATOR}#{ENV['PATH']}"

describe 'optimus.rb' do
  include Aruba::Api

  let(:help_message) do
    <<-eos
Missing options: file, pipeline
Usage: optimus.rb --file /path/to/config.yml --pipeline pipeline_identifier
    -f, --file FILE                  Path to YAML config file
    -p, --pipeline PIPELINE          Identifier string of pipeline to run
    -h, --help                       Show this message
    eos
  end

  let(:finished) do
    <<-eos
{:a=>
  {:class=>"OptimusPrime::Sources::LocalCsv",
   :params=>
    {:file_path=>"../../spec/supports/csv/local_csv_source_sample.csv"},
   :next=>["c"]},
 :b=>
  {:class=>"OptimusPrime::Sources::LocalCsv",
   :params=>
    {:file_path=>"../../spec/supports/csv/local_csv_source_pipe.csv",
     :col_sep=>"|"},
   :next=>["c"]},
 :c=>
  {:class=>"OptimusPrime::Destinations::LocalCsv",
   :params=>
    {:fields=>
      ["FirstName",
       "LastName",
       "Title",
       "ReportsTo.Email",
       "Birthdate",
       "Description"],
     :file_path=>"../../spec/supports/csv/destination.csv"}}}
Pipeline finished.
    eos
  end

  let(:aruba_config_path) { '../../spec/supports/config/aruba-test-config.yml' }

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
      run_simple "optimus.rb -p test_pipeline -f #{aruba_config_path}",
                 false
    end

    it 'should print out the finished output when arguments are given ' do
      all_output.should eq finished
    end

    it 'should write in the destination csv' do
      destination = File.open('spec/supports/csv/destination.csv', 'r')
      destination.readlines.size.should_not eq 0
    end
  end

  describe 'Help output' do
    it 'should print out help message if no arguments are given' do
      run_simple 'optimus.rb', false
      all_output.should eq help_message
    end
  end

  describe 'Missing Pipeline' do
    it 'should raise a Pipeline not found exception when the specified pipeline is not found' do
      run_simple "optimus.rb  -p inexistent_pipeline -f #{aruba_config_path}", false
      all_output.should include('Pipeline not found (RuntimeError)')
    end
  end
end
