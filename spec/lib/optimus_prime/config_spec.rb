require 'spec_helper'

describe OptimusPrime::Config do
  context "load config" do
    it 'should successfully load config' do
      config = OptimusPrime::Config.new(file_path: "spec/supports/sources.yml")
    end

    it 'should not allow duplicate' do
      expect { OptimusPrime::Config.new(file_path: 'spec/supports/duplicate.yml') }.to raise_error
    end
  end
end