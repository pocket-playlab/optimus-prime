require 'spec_helper'

RSpec.describe OptimusPrime::Destinations::BigqueryDynamic do
  let(:sample_base) { 'spec/supports/bigquery_dynamic/sample_data/' }
  let(:input) { YAML.load_file(sample_base + 'input.yml') }
  let(:tables) do
    YAML.load_file(sample_base + 'tables.yml')
      .each { |name, schema| schema.concat(template['schema']['fields']) }
  end
  let(:template) { YAML.load_file(sample_base + 'resource_template.yml') }
  let(:table_id) { { 'fields' => ['game', 'version'] } }
  let(:type_map) { YAML.load_file(sample_base + 'types.yml') }
  let(:logfile)  { '/tmp/bq-dynamic-destination.log' }

  let(:destination) do
    destination = OptimusPrime::Destinations::BigqueryDynamic.new(
      client_email: ENV.fetch('GOOGLE_CLIENT_EMAIL', 'test@developer.gserviceaccount.com'),
      private_key: ENV.fetch('GOOGLE_PRIVATE_KEY', File.read('spec/supports/key')),
      resource_template: template,
      table_id: table_id,
      type_map: type_map
    )
    destination.logger = Logger.new(logfile)
    destination
  end
  let(:client)   { destination.send :client }
  let(:bigquery) { client.discovered_api 'bigquery', 'v2' }

  def execute(method, params: {}, body: nil)
    client.execute api_method: method,
                   parameters: params.merge('projectId' => 'ppl-analytics',
                                            'datasetId' => 'test'),
                   body_object: body
  end

  def determine_table_of(record)
    table_fields_of(record).join('_')
      .prepend(prefix).concat(suffix)
      .downcase.gsub('.', '_')
  end

  def table_fields_of(record)
    record.collect do |field, value|
      value.to_s if table_id['fields'].include? field.to_s
    end.compact
  end

  def prefix
    table_id['prefix'].nil? ? '' : "#{table_id['prefix']}_"
  end

  def suffix
    table_id['suffix'].nil? ? '' : "_#{table_id['suffix']}"
  end

  def create_table(table_id)
    body = Marshal.load(Marshal.dump(template))
    yield(body) if block_given?
    body['tableReference']['tableId'] = table_id
    execute bigquery.tables.insert, body: body
  end

  def delete_table(table_id)
    execute bigquery.tables.delete, params: { 'tableId' => table_id }
  end

  def reset
    destination.run_with(input.dup)
    tables.each { |name, schema| delete_table name }
    create_table('joisecubes_1_1') { |body| body['schema']['fields'] = tables['joisecubes_1_1'] }
    create_table('gunjlecubes_1_1')
  end

  def map_rows_to_schema(schema, rows)
    rows.map do |row|
      Hash[row['f'].map.with_index do |field, index|
        value = field['v'] ? field['v'].convert_to(schema[index]['type']) : field['v']
        [schema[index]['name'], value]
      end].reject { |key, value| value.nil? }
    end
  end

  def download_data(tid)
    data = JSON.parse(execute(bigquery.tabledata.list, params: { 'tableId' => tid }).body)['rows']
    schema = JSON.parse(execute(bigquery.tables.get,
                                params: { 'tableId' => tid })
                        .body)['schema']['fields']
    map_rows_to_schema(schema, data)
  end

  def test_data(tid)
    output_data = download_data(tid)
    input_data = input.select { |i| determine_table_of(i) == tid }
    expect(output_data).to match_array input_data
  end

  def test_schema(tid)
    response = execute bigquery.tables.get, params: { 'tableId' => tid }
    expect(response.status).to eq(200)
    body = JSON.parse response.body
    expect(body['schema']['fields']).to match_array tables[tid]
  end

  before(:each) { File.truncate(logfile, 0) if File.exist?(logfile) }

  context 'not exceeding rate limits' do
    before :each do
      VCR.use_cassette('bigquery_dynamic/reset-and-run') { reset }
      # sleep 60 # Needed when running on the real bigquery
    end

    context 'table exists with complete schema' do
      it 'should have no schema changes' do
        VCR.use_cassette('bigquery_dynamic/complete-schema') do
          test_schema('joisecubes_1_1')
        end
      end

      it 'should contain two rows' do
        VCR.use_cassette('bigquery_dynamic/complete-data') do
          test_data('joisecubes_1_1')
        end
      end
    end

    context 'table exists with incomplete schema' do
      it 'should have schema changes' do
        VCR.use_cassette('bigquery_dynamic/incomplete-schema') do
          test_schema('gunjlecubes_1_1')
        end
      end

      it 'should contain two rows' do
        VCR.use_cassette('bigquery_dynamic/incomplete-data') do
          test_data('gunjlecubes_1_1')
        end
      end
    end

    context 'table does not exist' do
      it 'should be created' do
        VCR.use_cassette('bigquery_dynamic/new-schema') do
          test_schema('wormcubes_1_1')
        end
      end

      it 'should contain two rows' do
        VCR.use_cassette('bigquery_dynamic/new-data') do
          test_data('wormcubes_1_1')
        end
      end
    end
  end

  context 'exceeding rate limits' do
    it 'should insert all rows correctly' do
      VCR.use_cassette('bigquery_dynamic/limit-exceeded') do
        sample = { 'game' => 'fat_cubes_limits', 'version' => '1.1', 'event' => 'nothing' }
        input = 30_000.times.map { sample }
        destination.run_with(input)
        # sleep 120 # Needed when running on the real bigquery
        data = download_data('fat_cubes_limits_1_1')
        data.each do |event|
          expect(event).to include('game', 'version', 'event')
        end
      end
    end
  end

  context 'handling errors' do
    let(:insert_all_regex) do
      %r{https://www.googleapis.com/bigquery/v2/projects/.*/datasets/.*/tables/.*/insertAll}i
    end

    def stub_insert_all(responses)
      allow_any_instance_of(Object).to receive(:sleep)
      stub_request(:post, insert_all_regex).to_return(responses)
    end

    let(:full_success) do
      {
        status: 200,
        body: { 'kind' => 'bigquery#tableDataInsertAllResponse' }.to_json
      }
    end

    let(:partial_success) do
      {
        status: 200,
        body: {
          'kind' => 'bigquery#tableDataInsertAllResponse',
          'insertErrors' => [
            {
              'index'  => 0,
              'errors' => [{ 'reason' => 'invalid', 'message' => 'no such field' }]
            }
          ]
        }.to_json
      }
    end

    let(:sample_data) do
      [{ 'game' => 'joisecubes', 'version' => 1.1, 'event' => 'addgold', 'amount' => 28 }]
    end

    around(:each) do |example|
      VCR.use_cassette('bigquery_dynamic/error-handling') do
        delete_table('joisecubes_1_1')
        create_table('joisecubes_1_1')
        example.run
      end
    end

    context 'response with 50x status' do
      let(:res50x) { { status: 502, body: { 'kind' => 'foo', 'insertErrors' => [] }.to_json } }
      it 'succeeds within 5 retries' do
        stub_insert_all([res50x, res50x, res50x, res50x, full_success])
        expect { destination.run_with(sample_data) }.to_not raise_error
      end

      it 'fails after 5 retries' do
        stub_insert_all([res50x])
        expect { destination.run_and_raise(sample_data) }.to raise_error
      end
    end

    context 'response with 40x status' do
      let(:res40x) { { status: 403, body: { stub: 'stub' }.to_json } }
      it 'fails immediately' do
        stub_insert_all([res40x])
        expect { destination.run_and_raise(sample_data) }.to raise_error
      end
    end

    context 'partial insertion success' do
      it 'retries once and continues on success' do
        stub_insert_all([partial_success, full_success])
        expect { destination.run_with(sample_data) }.to_not raise_error
      end

      it 'continues after one retry with invalid records logged' do
        stub_insert_all([partial_success])
        destination.run_with(sample_data)
        expect(File.read(logfile)).to include('Insertion Error')
      end
    end
  end
end
