require 'spec_helper'
require 'optimus_prime/destinations/local_csv'
require 'fakefs/safe'

RSpec.describe OptimusPrime::Destinations::LocalCsv do
  let(:file_path) { './test.csv' }

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

  let(:fields) { input.first.keys }

  it 'should write out a csv file' do
    FakeFS.activate!

    dest = OptimusPrime::Destinations::LocalCsv.new fields: fields,
                                                    file_path: file_path

    input.each { |record| dest.write record }
    dest.close

    data = CSV.read file_path, converters: :all

    header = data.shift
    expect(header).to eq dest.fields
    expect(data.map { |row| header.zip(row).to_h })
      .to eq input.map { |row| row.select { |k, v| header.include? k }}

    FakeFS.deactivate!
  end
end
