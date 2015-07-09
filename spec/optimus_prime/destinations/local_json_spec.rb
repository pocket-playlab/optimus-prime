require 'spec_helper'
require 'fakefs/safe'

RSpec.describe OptimusPrime::Destinations::LocalJson do
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
  let(:extra_record) { { 'userid' => 'batman', 'game' => 'jungle cubes', 'version' => 2.00 } }
  let(:fields) { input.first.keys }
  let(:path) { 'test.json' }
  let(:step) { OptimusPrime::Destinations::LocalJson.new(file_path: path) }

  around(:each) do |example|
    FakeFS::FileSystem.clear
    FakeFS { example.run }
  end

  def run_test(with_extra: false)
    data = File.open(path) { |f| f.map { |line| JSON.parse line } }
    input.push(extra_record) if with_extra
    expect(data).to match_array input
  end

  it 'should write out a json file' do
    step.run_with(input.dup)
    run_test
  end

  it 'should append to an existing file' do
    step.run_with(input.dup)
    OptimusPrime::Destinations::LocalJson.new(file_path: path).run_with([extra_record])
    run_test(with_extra: true)
  end
end
