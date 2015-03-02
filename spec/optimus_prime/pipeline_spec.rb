require 'spec_helper'

class TestSource < OptimusPrime::Source
  def initialize(data:)
    @data = data
  end

  def each
    @data.each { |i| yield i }
  end
end

class IncrementStep < OptimusPrime::Destination
  def write(data)
    sleep 0.1
    send data + 1
  end
end

class DoubleStep < OptimusPrime::Destination
  def write(data)
    send data * 2
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
    OptimusPrime::Pipeline.new(
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
        class: 'DoubleStep',
        next: ['e']
      },
      d: {
        class: 'IncrementStep',
        next: ['e']
      },
      e: {
        class: 'DoubleStep',
        next: ['f', 'g']
      },
      f: {
        class: 'TestDestination'
      },
      g: {
        class: 'TestDestination'
      }
    )
  end

  describe '#start' do
    it 'should run the pipeline' do
      pipeline.start
      expect(pipeline.started?).to be true
      expect(pipeline.finished?).to be false
      pipeline.join
      expect(pipeline.finished?).to be true
      expected = (4..40).step(4).to_a + (202..222).step(2).to_a
      pipeline.steps.values_at(:f, :g).each do |destination|
        expect(destination.written).to match_array expected
      end
    end

    it 'should fail when called twice' do
      pipeline.start
      expect { pipeline.start }.to raise_error
    end
  end
end
