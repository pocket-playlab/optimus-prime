require 'spec_helper'
require 'optimus_prime/destinations/local_csv'
require 'fakefs/safe'

RSpec.describe OptimusPrime::Destinations::LocalCsv do
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
  let(:path) { 'test.csv' }
  let(:step) { OptimusPrime::Destinations::LocalCsv.new(fields: fields, file_path: path) }

  def run_test(with_extra: false)
    data = CSV.read(path, converters: :all)
    header = data.shift
    expect(header).to match_array fields
    input.push(extra_record) if with_extra
    expect(data.map { |row| header.zip(row).to_h })
      .to match_array input.map { |row| row.select { |k, v| header.include? k } }
  end

  around(:each) do |example|
    FakeFS::FileSystem.clear
    FakeFS { example.run }
  end

  it 'should write out a csv file' do
    step.run_with(input.dup)
    run_test
  end

  it 'should not write header when appending to existing file' do
    step.run_with(input.dup)
    OptimusPrime::Destinations::LocalCsv.new(fields: fields, file_path: path).run_with([extra_record])
    run_test(with_extra: true)
  end
end
