require 'spec_helper'
require 'optimus_prime/streams/file_streams/newline_json_gzipped'

RSpec.describe OptimusPrime::Streams::FileStreams::NewlineJsonGzipped do
  let(:mpf) { 100 }
  let(:sample) { { 'foo' => 'bar', 'baz' => 'quux' } }
  let(:stream) { OptimusPrime::Streams::FileStreams::NewlineJsonGzipped.new('/tmp', mpf) }
  after(:each) { File.delete(*Dir['/tmp/*.jgz']) }

  context '#initialize' do
    context 'when no options given' do
      let(:default_opts) { [Zlib::BEST_COMPRESSION, Zlib::DEFAULT_STRATEGY] }
      it 'creates a stream with the default options' do
        stream = OptimusPrime::Streams::FileStreams::NewlineJsonGzipped.new('/tmp', mpf)
        stream.close
        expect(stream.instance_variable_get :@zoptions).to match_array default_opts
      end
    end

    context 'when options are supplied' do
      context 'with valid values' do
        it 'creates a stream with the supplied options' do
          opts = { level: Zlib::BEST_SPEED, strategy: Zlib::FIXED }
          stream = OptimusPrime::Streams::FileStreams::NewlineJsonGzipped.new('/tmp', mpf, opts)
          stream.close
          expect(stream.instance_variable_get :@zoptions).to match_array opts.values
        end
      end

      context 'with invalid values' do
        it 'raises an error' do
          opts = { level: -2, strategy: 5 }
          expect do
            OptimusPrime::Streams::FileStreams::NewlineJsonGzipped.new('/tmp', mpf, opts)
          end.to raise_error(Zlib::StreamError)
        end
      end

      context 'with missplled or extra options' do
        it 'raises an error' do
          opts = { level: Zlib::BEST_SPEED, strategy: Zlib::FIXED, memory: Zlib::MAX_MEM_LEVEL }
          expect do
            OptimusPrime::Streams::FileStreams::NewlineJsonGzipped.new('/tmp', mpf, opts)
          end.to raise_error(Zlib::StreamError)
        end
      end
    end
  end

  context 'each chunk' do
    before :each do
      @file = mpf.times.map { stream << sample }.compact.first
      stream.close
      @gz = Zlib::GzipReader.open(@file)
    end

    after(:each) { @gz.close }

    it 'is a valid gzip file' do
      expect { @gz.read }.to_not raise_error
    end

    it 'is newline delimitered' do
      content = @gz.read
      expect(content.lines.length).to be > 1
    end

    it 'has exactly the expected number of records' do
      content = @gz.read
      expect(content.lines.length).to eq mpf
    end

    it 'contains valid JSON objects' do
      content = @gz.read
      expect { content.each_line { |line| JSON.parse(line.strip) } }.to_not raise_error
    end
  end

  context 'chunking' do
    before :each do
      files = total.times.map { stream << sample }
      files << stream.close
      @files = files.compact
    end

    context 'with the count of all records = max_per_file * X' do
      let(:total) { 10 * mpf }

      after :each do
        File.delete(*Dir['*.jgz'])
      end

      it 'generates X number of files' do
        expect(@files.length).to eq total / mpf
      end

      it 'makes each file contain max_per_file records' do
        @files.each do |file|
          gz = Zlib::GzipReader.open(file)
          content = gz.read
          gz.close
          expect(content.lines.length).to eq(mpf)
        end
      end
    end

    context 'with the count of all records = (max_per_file * X) + Y' do
      let(:total) { 10 * mpf + 5 }

      it 'generates X + 1 files' do
        expect(@files.length).to eq total / mpf + 1
      end

      it 'makes all files but the last one contain max_per_file records' do
        @files.first(@files.length - 1).each do |file|
          gz = Zlib::GzipReader.open(file)
          content = gz.read
          gz.close
          expect(content.lines.length).to eq(mpf)
        end
      end

      it 'makes the last file contain Y records' do
        gz = Zlib::GzipReader.open(@files.last)
        content = gz.read
        gz.close
        expect(content.lines.length).to eq(total % mpf)
      end
    end
  end
end
