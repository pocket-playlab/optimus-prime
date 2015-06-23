require 'spec_helper'

module OptimusPrime
  module Sources
    class Test < OptimusPrime::Source
      def initialize(data:, delay: 0)
        @data = data
        @delay = delay
      end

      def each
        sleep @delay
        @data.each { |i| yield i }
      end
    end
  end

  module Destinations
    class Test < OptimusPrime::Destination
      attr_reader :written
      def write(record)
        @received ||= []
        @received << record
      end

      def finish
        @written = @received
      end
    end
  end
end

class IncrementStep < OptimusPrime::Destination
  def write(data)
    sleep 0.1
    data['value'] += 1
    push data
  end
end

class DoubleStep < OptimusPrime::Destination
  def write(data)
    data['value'] *= 2
    push data
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

  def data_for(range)
    range.map do |i|
      { 'value' => i }
    end
  end

  let(:steps) do
    {
      a: {
        class: 'OptimusPrime::Sources::Test',
        params: { data: data_for(1..10), delay: 1 },
        next: ['c']
      },
      b: {
        class: 'OptimusPrime::Sources::Test',
        params: { data: data_for(100..110) },
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
        class: 'OptimusPrime::Destinations::Test'
      },
      g: {
        class: 'OptimusPrime::Destinations::Test'
      }
    }
  end

  let(:modules) do
    {
      persistence: {
        options: { dsn: 'sqlite:test.db' }
      },
      exceptional: {
        adapter: 'Sentry',
        options: { dsn: 'test' }
      }
    }
  end

  let(:pipeline) do
    OptimusPrime::Pipeline.new(steps)
  end

  let(:pipeline_with_name) do
    OptimusPrime::Pipeline.new(steps, 'super_pipeline')
  end

  let(:pipeline_with_modules) do
    OptimusPrime::Pipeline.new(steps, :my_pipeline, modules)
  end

  describe '#start' do
    it 'should run the pipeline' do
      pipeline.start
      expect(pipeline.started?).to be true
      expect(pipeline.finished?).to be false
      pipeline.wait
      expect(pipeline.finished?).to be true
      expected = (4..40).step(4).to_a + (202..222).step(2).to_a
      pipeline.steps.values_at(:f, :g).each do |destination|
        actual = destination.written.map { |record| record['value'] }
        expect(actual).to match_array expected
      end
    end

    it 'should fail when called twice' do
      pipeline.start
      expect { pipeline.start }.to raise_error
    end
  end

  describe 'modules' do
    it 'runs the pipeline' do
      pipeline_with_modules.operate
      expect(pipeline_with_modules.finished?).to be true

      expected = (4..40).step(4).to_a + (202..222).step(2).to_a
      pipeline_with_modules.steps.values_at(:f, :g).each do |destination|
        actual = destination.written.map { |record| record['value'] }
        expect(actual).to match_array expected
      end
    end

    it 'loads the persistence module' do
      pipeline_with_modules.operate
      expect(pipeline_with_modules.module_loader.persistence).to_not be nil
    end

    it 'loads the exceptional module' do
      pipeline_with_modules.operate
      expect(pipeline_with_modules.module_loader.exceptional).to_not be nil
    end

    describe 'persistence' do

      let(:base) do
        OptimusPrime::Modules::Persistence::Base.new(dsn: 'sqlite:listener_test.db')
      end

      describe 'started' do

        it 'receives the pipeline started event' do
          pipeline.subscribe(base.listener)
          expect(base.listener).to receive(:pipeline_started).with(pipeline)
          pipeline.operate
        end

        it 'saves the pipeline in DB' do
          pipeline_with_name.subscribe(base.listener)
          pipeline_with_name.start
          operation = base.db[:operations].where(pipeline_id: pipeline_with_name.name).first
          expect(operation[:pipeline_id]).to eq pipeline_with_name.name
          expect(operation[:status]).to eq 'started'
        end

      end

      describe 'finished' do

        it 'receives the pipeline finished event' do
          pipeline.subscribe(base.listener)
          expect(base.listener).to receive(:pipeline_finished).with(pipeline)
          pipeline.operate
        end

        it 'updates the pipeline in DB when finished' do
          pipeline_with_name.subscribe(base.listener)
          pipeline_with_name.start
          operation = base.db[:operations].where(pipeline_id: pipeline_with_name.name).first
          pipeline_with_name.join
          updated = base.db[:operations].where(id: operation[:id]).first
          expect(updated[:pipeline_id]).to eq pipeline_with_name.name
          expect(updated[:status]).to eq 'finished'
          expect(updated[:error]).to eq nil
        end

      end

      describe 'failed' do

        it 'receives the pipeline failed event' do
          pipeline.subscribe(base.listener)
          expect(base.listener).to receive(:pipeline_failed)
          allow(pipeline).to receive(:join).and_raise('random exception')
          expect { pipeline.operate }.to raise_error('random exception')
        end

        it 'updates the pipeline in DB when failed' do
          allow(pipeline_with_name).to receive(:join).and_raise('random exception')
          pipeline_with_name.subscribe(base.listener)
          expect { pipeline_with_name.operate }.to raise_error('random exception')
          operation = base.db[:operations].where(pipeline_id: pipeline_with_name.name).first
          expect(operation[:pipeline_id]).to eq pipeline_with_name.name
          expect(operation[:status]).to eq 'failed'
          expect(operation[:error]).to include 'random exception'
        end

      end

    end

  end

end
