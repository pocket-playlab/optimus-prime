require 'spec_helper'
# it should throw exception if columns method is not overridden

# it should throw exception if get_data method is not overridden

# it should have columns attribute which returns hash where keys are column names and values are types

# it should have a get_data method which returns data in array of arrays format

describe OptimusPrime::Destination, '#columns' do
  it "should raise" do
    expect{OptimusPrime::Destination.columns}.to raise_error()
  end
end

describe OptimusPrime::Destination, '#put_data' do
  it "should raise" do
    expect{OptimusPrime::Destination.put_data}.to raise_error()
  end
end
