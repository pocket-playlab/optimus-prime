require 'spec_helper'

class TestSource < OptimusPrime::Source
  def initialize(data:)
    @data = data
  end

  def each
    @data.each { |i| yield i }
  end
end

class IncrementTransform < OptimusPrime::Transform
  def transform(data)
    push data + 1
  end
end

class DoubleTransform < OptimusPrime::Transform
  def transform(data)
    push data * 2
  end
end

class TestDestination < OptimusPrime::Destination
  attr_reader :written
  def write(record)
    @received ||= []
    @received << record
  end

  def close
    @written = @received
  end
end

describe OptimusPrime::Pipeline do

  #     a   b
  #     |   |
  #     c   d
  #      \ /
  #       e
  #      / \
  #     f   g

  let(:pipeline) do
    OptimusPrime::Pipeline.new({
      a: {
        class: 'TestSource',
        params: { data: (1..10).to_a },
        next: ['c']
      },
      b: {
        class: 'TestSource',
        params: { data: (100..110).to_a },
        next: ['d']
      },
      c: {
        class: 'DoubleTransform',
        next: ['e']
      },
      d: {
        class: 'IncrementTransform',
        next: ['e']
      },
      e: {
        class: 'DoubleTransform',
        next: ['f', 'g']
      },
      f: {
        class: 'TestDestination'
      },
      g: {
        class: 'TestDestination'
      }
    })
  end

  describe '#sources' do

    it 'should only include sources' do
      expect(pipeline.sources.keys).to match_array [:a, :b]
    end

    it 'should instantiate a Source instance for each source' do
      pipeline.sources.values.each do |source|
        expect(source).to be_a OptimusPrime::Source
      end
    end

  end

  describe '#destinations' do

    it 'should only include destinations' do
      expect(pipeline.destinations.keys).to match_array [:f, :g]
    end

    it 'should instantiate a Destination instance for each destination' do
      pipeline.destinations.values.each do |destination|
        expect(destination).to be_a OptimusPrime::Destination
      end
    end

  end

  describe '#transforms' do

    it 'should only include transforms' do
      expect(pipeline.transforms.keys).to match_array [:c, :d, :e]
    end

    it 'should instantiate a Transform instance for each transform' do
      pipeline.transforms.values.each do |transform|
        expect(transform).to be_a OptimusPrime::Transform
      end
    end

  end

  describe '#start' do

    it 'should run the pipeline' do
      pipeline.start
      pipeline.join
      expected = (4..40).step(4).to_a + (202..222).step(2).to_a
      pipeline.destinations.values.each do |destination|
        expect(destination.written).to match_array expected
      end
    end

    it 'should fail when called twice' do
      pipeline.start
      expect { pipeline.start }.to raise_error
    end

  end

end
