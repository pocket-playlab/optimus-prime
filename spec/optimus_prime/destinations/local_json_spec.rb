require 'spec_helper'
require 'optimus_prime/destinations/local_json'
require 'fakefs/safe'

RSpec.describe OptimusPrime::Destinations::LocalJson do
  before(:all) do
    FakeFS.activate!
  end

  after(:all) do
    FakeFS.deactivate!
  end

  let(:input) do
    [
      { 'userid' => 'rick', 'game' => 'juice cubes', 'version' => 1.03 },
      { 'userid' => 'opal', 'game' => 'juice cubes', 'version' => 1.03 },
      { 'userid' => 'omar', 'game' => 'juice cubes', 'version' => 1.03 },
      { 'userid' => 'jakob', 'game' => 'juice cubes', 'version' => 1.03 },
      { 'userid' => 'thomas', 'game' => 'juice cubes', 'version' => 1.03 },
      { 'userid' => 'ton', 'game' => 'juice cubes', 'version' => 1.03 },
      { 'userid' => 'santa', 'game' => 'juice cubes', 'version' => 1.03 },
    ]
  end

  let(:append_data) do
    [
      { 'userid' => 'batman', 'game' => 'jungle cubes', 'version' => 2.00 }
    ]
  end

  let(:fields) { input.first.keys }

  def write_out_json_file(path)
    dest = OptimusPrime::Destinations::LocalJson.new file_path: path
    input.each { |record| dest.write record }
    dest.close
  end

  def append_json_file(path)
    dest = OptimusPrime::Destinations::LocalJson.new file_path: path
    append_data.each { |record| dest.write record }
    dest.close
  end

  it 'should write out a json file' do
    path = 'test.json'
    write_out_json_file path
    data = File.open(path) do |f|
      f.map { |line| JSON.parse line }
    end
    expect(data).to eq input
  end

  it 'should append to an existing file' do
    path = 'test-append.json'
    write_out_json_file path
    append_json_file path
    data = File.open(path) do |f|
      f.map { |line| JSON.parse line }
    end
    expect(data).to eq(input + append_data)
  end
end
