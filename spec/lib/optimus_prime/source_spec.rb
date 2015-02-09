require 'spec_helper'
# it should throw exception if columns method is not overridden

# it should throw exception if get_data method is not overridden

# it should have columns attribute which returns hash where keys are column names and values are types

# it should have a retrieve_data method which returns data in array of arrays format

describe OptimusPrime::Source, '#columns' do
  it "should raise" do
    expect{OptimusPrime::Source.columns}.to raise_error
  end
end

describe OptimusPrime::Source, '#retrieve_data' do
  it "should raise" do
    expect{OptimusPrime::Source.retrieve_data}.to raise_error
  end
end

class OptimusPrime::Source::Test < OptimusPrime::Source
end

describe OptimusPrime::Source::Test, '#columns' do
  it "should raise" do
    expect{OptimusPrime::Source::Test.columns}.to raise_error
  end
end

describe OptimusPrime::Source::Test, '#retrieve_data' do
  it "should raise" do
    expect{OptimusPrime::Source::Test.retrieve_data}.to raise_error
  end
end

class OptimusPrime::Source::Test < OptimusPrime::Source

  def implement_retrieve_data
    @data = [
      [ 1, "rick",  100],
      [ 2, "omar",  200],
      [ 3, "em",    300],
      [ 4, "prair", 400],
    ]
  end

end

describe OptimusPrime::Source::Test, '#columns' do
  it "should return hash as expected" do
    expected = { 'id': 'Integer', 'name': 'String', 'gold': 'Integer' }

    test_src = OptimusPrime::Source::Test.new({ 'id': 'Integer', 'name': 'String', 'gold': 'Integer' })
    expect(test_src.columns).to eq expected
  end
end

describe OptimusPrime::Source::Test, '#get_data' do
  it "should return expected data" do
    expected = [
      [ 1, "rick",  100],
      [ 2, "omar",  200],
      [ 3, "em",    300],
      [ 4, "prair", 400],
    ]

    test_src = OptimusPrime::Source::Test.new({ 'id': 'Integer', 'name': 'String', 'gold': 'Integer' })
    expect(test_src.retrieve_data).to eq expected
  end

  it "should return expected data" do
    expected = [
      [ 1, "rick",  100],
      [ 2, "omar",  200],
      [ 3, "em",    300],
      [ 4, "prair", 400],
    ]

    test_src = OptimusPrime::Source::Test.new({'col': nil})

    expect { test_src.retrieve_data }.to raise_error
  end
end
