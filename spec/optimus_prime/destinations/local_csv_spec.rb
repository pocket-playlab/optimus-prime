require 'spec_helper'
require 'optimus_prime/destinations/local_csv'
require 'fakefs/safe'

RSpec.describe OptimusPrime::Destinations::LocalCsv do
  before(:all) do
    FakeFS.activate!
  end


  after(:all) do
    FakeFS.deactivate!
  end

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

  let(:append_data) do
    [
        { 'userid' => 'batman', 'game' => 'jungle cubes', 'version' => 2.00 }
    ]
  end

  let(:fields) { input.first.keys }

  def write_out_csv_file
    dest = OptimusPrime::Destinations::LocalCsv.new fields: fields,
                                                    file_path: file_path

    input.each { |record| dest.write record }
    dest.close
  end

  def append_csv_file
    dest = OptimusPrime::Destinations::LocalCsv.new fields: fields,
                                                    file_path: file_path,
                                                    append_mode: true

    append_data.each { |record| dest.write record }
    dest.close
  end

  it 'should write out a csv file' do
    write_out_csv_file
    data = CSV.read file_path, converters: :all
    header = data.shift
    expect(header).to eq fields
    expect(data.map { |row| header.zip(row).to_h })
      .to eq input.map { |row| row.select { |k, v| header.include? k }}
  end

  it 'should not write header when appending to existing file' do
    write_out_csv_file
    append_csv_file
    data = CSV.read file_path, converters: :all
    header = data.shift
    expect(header).to eq fields

    expect(data.map { |row| header.zip(row).to_h })
        .to eq input.push(append_data).flatten.map { |row| row.select { |k, v| header.include? k }}
  end
end
