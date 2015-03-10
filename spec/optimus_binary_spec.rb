require 'spec_helper'
require 'aruba'
require 'aruba/api'
require 'pathname'

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

  after(:each) do
    File.open('spec/supports/csv/destination.csv', 'w') { |file| file.truncate(0) }
  end

  it 'should run' do
    run_simple 'optimus.rb -p test_pipeline -f ../../spec/supports/config/aruba-test-config.yml',
               false
    all_output.should eq finished
  end

  describe 'Help output' do
    it 'should print out help message if no arguments are given' do
      run_simple 'optimus.rb', false
      all_output.should eq help_message
    end
  end
end
