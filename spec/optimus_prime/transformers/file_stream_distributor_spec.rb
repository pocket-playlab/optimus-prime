require 'spec_helper'
require 'optimus_prime/transformers/file_stream_distributor'
require 'optimus_prime/streams/file_streams/newline_json_gzipped'

RSpec.describe OptimusPrime::Transformers::FileStreamDistributor do
  let(:max_per_file) { 10 }
  let(:count_per_cat) { 60 }
  let(:path_template) { 'optimuspec/jsondistributor/%{Version}' }
  let(:category_template) { 'sample_%{Version}' }
  let(:base_path) { '/tmp' }
  let(:sample) { { 'Event' => 'LevelUp', 'PlayerID' => '123456' } }
  let(:cat_ver) do
    { 'sample_1_2_3' => '1.2.3', 'sample_1_0_3' => '1.0.3', 'sample_1_4_5' => '1.4.5' }
  end
  let(:input) do
    count_per_cat.times.map do
      cat_ver.each.map { |key, value| sample.merge('Version' => value) }
    end.flatten.shuffle
  end
  let(:transformer) do
    OptimusPrime::Transformers::FileStreamDistributor.new(
      path_template: path_template, base_path: base_path,
      category_template: category_template,  max_per_file: max_per_file,
      stream_type: 'OptimusPrime::Streams::FileStreams::NewlineJsonGzipped'
    )
  end
  after(:each) do
    p = Pathname.new(File.join(base_path, 'optimuspec'))
    p.rmtree if p.exist?
  end

  context 'with invalid data' do
    it 'raises an error' do
      output = []
      transformer.output << output
      input.sample.delete('Version')
      expect { input.each { |record| transformer.write(record) } }.to raise_error(KeyError)
      transformer.close
    end
  end

  context 'with valid data' do
    it 'creates the expected number of files' do
      output = []
      transformer.output << output
      input.each { |record| transformer.write(record) }
      transformer.close
      expect(output.compact.length).to eq 18
    end

    it 'creates the expected number of files per category' do
      output = []
      transformer.output << output
      input.each { |record| transformer.write(record) }
      transformer.close
      results = cat_ver.each.map { |k, v| [k, []] }.to_h
      output.compact.each do |pair|
        results[pair[:category]] << pair[:file]
      end
      results.each do |category, files|
        expect(files.length).to eq 6
      end
    end

    it 'directs records to the correct categories' do
      output = []
      transformer.output << output
      input.each { |record| transformer.write(record) }
      transformer.close
      output.compact.each do |pair|
        category = pair[:category]
        file = pair[:file]
        gz = Zlib::GzipReader.open(File.join(base_path, file))
        content = gz.read
        gz.close
        content.each_line do |line|
          record = JSON.parse(line.strip)
          expect(record['Version']).to eq cat_ver[category]
        end
      end
    end
  end
end
