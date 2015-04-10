require 'spec_helper'
require 'optimus_prime/transformers/expand_json'

RSpec.describe OptimusPrime::Transformers::ExpandJSON do
  let(:logfile) { '/tmp/expand_json.log' }
  let(:invalid_input) { [{ 'a' => 'b', 'c' => 'd' }, { 'e' => 'f', 'g' => 'h' }] }

  def output_of(input)
    output = []
    transformer.output << output
    input.each { |record| transformer.write(record) }
    output
  end

  def logs
    File.read(logfile)
  end

  before do
    File.delete(logfile) if File.exist?(logfile)
    transformer.logger = Logger.new(logfile)
  end

  context 'with empty fields array' do
    let(:transformer) { OptimusPrime::Transformers::ExpandJSON.new(fields: []) }

    it 'pushes the same exact hash' do
      expect(output_of(invalid_input)).to match_array(invalid_input)
    end
  end

  context 'with invalid json in record fields' do
    let(:transformer) { OptimusPrime::Transformers::ExpandJSON.new(fields: ['a', 'e']) }

    it 'pushes nothing and logs an error' do
      expect(output_of(invalid_input)).to match_array([])
      expect(logs).to include('Cannot expand invalid JSON field')
    end
  end

  context 'with valid json and fields' do
    let(:simple_input) do
      [{ 'a' => 'b', 'c' => { 'foo' => 'bar', 'baz' => 'quux' }.to_json },
       { 'e' => 'f', 'g' => 'h' }]
    end

    let(:simple_output) do
      [{ 'a' => 'b', 'foo' => 'bar', 'baz' => 'quux' }, { 'e' => 'f', 'g' => 'h' }]
    end
    let(:transformer) { OptimusPrime::Transformers::ExpandJSON.new(fields: ['c']) }

    it 'expands json fields correctly' do
      expect(output_of(simple_input)).to match_array(simple_output)
    end
  end

  context 'with duplicate fields in the record and the json field' do
    let(:duplicates) do
      [
        { 'first' => 'second', 'third' => { 'first' => 'fourth' }.to_json },
        { 'first' => 'second', 'third' => { 'fourth' => 'fifth' }.to_json }
      ]
    end

    context 'when overwrite is enabled' do
      let(:transformer) do
        OptimusPrime::Transformers::ExpandJSON.new(fields: ['third'], overwrite: true)
      end
      let(:output_with_overwrite) do
        [{ 'first' => 'fourth' }, { 'first' => 'second', 'fourth' => 'fifth' }]
      end

      it 'overwrites the original values with the expanded ones' do
        expect(output_of(duplicates)).to match_array(output_with_overwrite)
      end
    end

    context 'when overwrite is disabled' do
      let(:transformer) do
        OptimusPrime::Transformers::ExpandJSON.new(fields: ['third'], overwrite: false)
      end
      let(:output_without_overwrite) do
        [{ 'first' => 'second' }, { 'first' => 'second', 'fourth' => 'fifth' }]
      end
      it 'keeps the original values and ignores the expanded ones' do
        expect(output_of(duplicates)).to match_array(output_without_overwrite)
      end
    end
  end
end
