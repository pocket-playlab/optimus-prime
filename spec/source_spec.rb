require_relative '../lib/optimus_prime.rb'

# it should throw exception if columns method is not overridden

# it should throw exception if get_data method is not overridden

# it should have columns attribute which returns hash where keys are column names and values are types

# it should have a retrieve_data method which returns data in array of arrays format

describe OptimusPrime::Source, '#columns' do
  it "should raise" do
    expect{OptimusPrime::Source.columns}.to raise_error()
  end
end

describe OptimusPrime::Source, '#retrieve_data' do
  it "should raise" do
    expect{OptimusPrime::Source.retrieve_data}.to raise_error()
  end
end

class OptimusPrime::Source::Test < OptimusPrime::Source
end

describe OptimusPrime::Source::Test, '#columns' do
  it "should raise" do
    expect{OptimusPrime::Source::Test.columns}.to raise_error()
  end
end

describe OptimusPrime::Source::Test, '#retrieve_data' do
  it "should raise" do
    expect{OptimusPrime::Source::Test.retrieve_data}.to raise_error()
  end
end

class OptimusPrime::Source::Test < OptimusPrime::Source
  def columns
    return {
      id:   "Integer",
      name: "String",
      gold: "Integer",
    }
  end
 
  def get_data
    return [
      [ 1, "rick",  100],
      [ 2, "omar",  200],
      [ 3, "em",    300],
      [ 4, "prair", 400],
    ]
  end
end


describe OptimusPrime::Source::Test, '#columns' do
  it "should return hash as expected" do
    expected = { id: "Integer", name: "String", gold: "Integer" }  

    test_src = OptimusPrime::Source::Test.new
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

    test_src = OptimusPrime::Source::Test.new
    expect(test_src.get_data).to eq expected
  end
end
