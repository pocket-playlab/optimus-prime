require 'spec_helper'

class TestSource < OptimusPrime::Source
  def initialize(data:)
    @data = data
  end

  def each
    @data.each { |i| yield i }
  end
end

class TestTransform < OptimusPrime::Transform
  def write(data)
    push data + 1
  end
end

class TestDestination < OptimusPrime::Destination
  attr_reader :written
  def write(record)
    @written ||= []
    @written << record
  end
end

describe OptimusPrime::Pipeline do

  let(:pipeline) do
    OptimusPrime::Pipeline.new({
      a: {
        class: 'TestSource',
        params: { data: (1..10).to_a },
        next: ['b']
      },
      b: {
        class: 'TestTransform',
        next: ['c']
      },
      c: {
        class: 'TestDestination'
      }
    })
  end

  describe '#sources' do

    it 'should only include sources' do
      expect(pipeline.sources.keys).to eq [:a]
    end

    it 'should instantiate a Source instance for each source' do
      pipeline.sources.values.each do |source|
        expect(source).to be_a OptimusPrime::Source
      end
    end

  end

  describe '#destinations' do

    it 'should only include destinations' do
      expect(pipeline.destinations.keys).to eq [:c]
    end

    it 'should instantiate a Destination instance for each destination' do
      pipeline.destinations.values.each do |destination|
        expect(destination).to be_a OptimusPrime::Destination
      end
    end

  end

  describe '#transforms' do

    it 'should only include transforms' do
      expect(pipeline.transforms.keys).to eq [:b]
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
      expect(pipeline.destinations[:c].written).to eq (2..11).to_a
    end

    it 'should fail when called twice' do
      pipeline.start
      expect { pipeline.start }.to raise_error
    end

  end

end
