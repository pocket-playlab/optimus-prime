require 'spec_helper'

RSpec.describe OptimusPrime::Transformers::ExpandHash do
  let(:logfile) { '/tmp/expand_hash.log' }
  let(:logger) { Logger.new(logfile) }
  let(:logs) { File.read(logfile) }
  let(:step_type) { OptimusPrime::Transformers::ExpandHash }


  before(:each) { File.delete(logfile) if File.exist?(logfile) }

  context 'when input contains no nested hash' do
    let(:input) { [{ 'a' => 'b', 'c' => 'd' }, { 'e' => 'f', 'g' => 'h' }] }

    context 'with empty fields array' do
      it 'pushes the same exact hash' do
        step = step_type.new(fields: [])
        expect(step.run_with(input.dup)).to match_array input
      end
    end

    context 'with non-empty fields array' do
      it 'pushes nothing and logs an error' do
        step = step_type.new(fields: ['a', 'e']).log_to(logger)
        expect(step.run_with(input)).to match_array []
        expect(logs).to include('Cannot expand invalid Hash field')
      end
    end
  end

  context 'with valid hash and fields' do
    let(:input) do
      [{ 'a' => 'b', 'c' => { 'foo' => 'bar', 'baz' => 'quux' } }, { 'e' => 'f', 'g' => 'h' }]
    end

    let(:output) do
      [{ 'a' => 'b', 'foo' => 'bar', 'baz' => 'quux' }, { 'e' => 'f', 'g' => 'h' }]
    end

    it 'expands hash fields correctly' do
      step = step_type.new(fields: ['c'])
      expect(step.run_with(input)).to match_array output
    end
  end

  context 'with duplicate fields in the record and the hash field' do
    let(:input) do
      [
        { 'first' => 'second', 'third' => { 'first' => 'fourth' } },
        { 'first' => 'second', 'third' => { 'fourth' => 'fifth' } }
      ]
    end

    context 'when overwrite is enabled' do
      let(:output) do
        [{ 'first' => 'fourth' }, { 'first' => 'second', 'fourth' => 'fifth' }]
      end

      it 'overwrites the original values with the expanded ones' do
        step = step_type.new(fields: ['third'], overwrite: true)
        expect(step.run_with(input)).to match_array output
      end
    end

    context 'when overwrite is disabled' do
      let(:output) do
        [{ 'first' => 'second' }, { 'first' => 'second', 'fourth' => 'fifth' }]
      end

      it 'keeps the original values and ignores the expanded ones' do
        step = step_type.new(fields: ['third'], overwrite: false)
        expect(step.run_with(input)).to match_array output
      end
    end
  end
end
