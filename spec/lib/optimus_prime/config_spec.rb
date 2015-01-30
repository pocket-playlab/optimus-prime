require 'spec_helper'

describe OptimusPrime::Config do

  context "load config" do
    it 'should successfully load config' do
      config = OptimusPrime::Config.new(file_path: "spec/supports/sources.yml")
      expect(config.sources.first.has_value?('installs_report')).to eq true
      expect(config.sources.last.has_value?('in_apps_report')).to eq true
    end

    it 'should error when found duplicate' do
      expect { OptimusPrime::Config.new(file_path: 'spec/supports/duplicate.yml') }.to raise_error
    end

    it 'should error when file not found' do
      expect { OptimusPrime::Config.new(file_path: 'empty.yml') }.to raise_error('file not found')
    end
  end
end