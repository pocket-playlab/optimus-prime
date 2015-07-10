require 'spec_helper'

class TestStep < OptimusPrime::Destination
  def write(record)
    push(record)
  end
end

class Listener
  def step_closed(step, step_class, consumed, produced)
  end
end

RSpec.describe OptimusPrime::Step do
  let(:input) { 10.times.map { { 'a' => 'b', 'c' => 'd' } } }
  let(:step) { TestStep.new }

  it 'publishes a step_closed event when closed' do
    l = Listener.new
    expect(l).to receive(:step_closed).with(step, TestStep, input.size, input.size)
    step.subscribe(l)
    step.run_with(input)
  end
end
