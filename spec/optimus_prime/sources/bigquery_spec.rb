require 'spec_helper'
require 'optimus_prime/sources/bigquery'

describe OptimusPrime::Sources::Bigquery do
  let(:response_rows) { JSON.parse(File.read('spec/supports/bigquery/src-response-rows.json')) }
  let(:project_id) { 'project-id' }
  let(:job_id) { 'job-id' }
  let(:sql) { 'SELECT Game, Platform, PercentComplete, MIN(Score) AS MinScore, IsTester FROM [dataset.table];' }
  let(:query_response) do
    {
      'kind' => 'bigquery#queryResponse',
      'schema' => {
        'fields' => [{ 'name' => 'Game', 'type' => 'STRING', 'mode' => 'NULLABLE' },
                     { 'name' => 'Platform', 'type' => 'STRING', 'mode' => 'NULLABLE' },
                     { 'name' => 'PercentComplete', 'type' => 'FLOAT', 'mode' => 'NULLABLE' },
                     { 'name' => 'MinScore', 'type' => 'INTEGER', 'mode' => 'NULLABLE' },
                     { 'name' => 'IsTester', 'type' => 'BOOLEAN', 'mode' => 'NULLABLE' }]
      },
      'jobReference' => { 'projectId' => project_id, 'jobId' => job_id },
      'totalRows' => response_rows.count.to_s,
      'rows' => response_rows,
      'totalBytesProcessed' => '1200',
      'jobComplete' => true,
      'cacheHit' => false
    }
  end

  let(:results) do
    [{ Game: nil, Platform: 'android', PercentComplete: 0.0, MinScore: 88_550, IsTester: true },
     { Game: 'a', Platform: nil, PercentComplete: 4.2, MinScore: 28_200, IsTester: false },
     { Game: 'b', Platform: 'android', PercentComplete: nil, MinScore: 47_325, IsTester: true },
     { Game: 'b', Platform: 'android', PercentComplete: 2.15, MinScore: nil, IsTester: false },
     { Game: 'b', Platform: 'android', PercentComplete: 42.9, MinScore: 128_175, IsTester: nil }]
    .map { |hash| hash.stringify_nested_symbolic_keys }
  end

  let(:step) do
    OptimusPrime::Sources::Bigquery.new(
      email: 'test@developer.gserviceaccount.com', key_file: 'test-privatekey.p12',
      project_id: project_id, sql: sql, pass_phrase: 'notasecret'
    ).suppress_log
  end

  def stub_get_query_results(rows, request_page_token, next_page_token)
    response = query_response.clone
    response['rows'] = rows
    response['pageToken'] = next_page_token if next_page_token
    allow(GoogleBigquery::Jobs).to receive(:getQueryResults)
      .with(project_id, job_id, request_page_token)
      .and_return(response)
  end

  before :each do
    allow(GoogleBigquery::Auth).to receive_message_chain(:new, :authorize).and_return(true)
  end

  context 'no result' do
    it 'should not have a result' do
      query_response['totalRows'] = '0'
      query_response.delete 'rows'
      allow(GoogleBigquery::Jobs).to receive(:query).and_return(query_response)
      expect(step.run_with.to_a).to be_empty
    end
  end

  context 'one page result' do
    it 'should yield all results' do
      allow(GoogleBigquery::Jobs).to receive(:query).and_return(query_response)
      expect(step.run_with.to_a).to eq(results)
    end
  end

  context 'multiple pages result' do
    it 'should yield all results' do
      incomplete_query_response = query_response.select do |k, v|
        ['kind', 'jobReference', 'jobComplete'].include? k
      end
      incomplete_query_response['jobComplete'] = false
      allow(GoogleBigquery::Jobs).to receive(:query).and_return(incomplete_query_response)

      stub_get_query_results response_rows.take(2), {}, '2'
      stub_get_query_results response_rows[2, 2], { pageToken: '2' }, '3'
      stub_get_query_results [response_rows.last], { pageToken: '3' }, nil

      expect(step.run_with.to_a).to eq(results)
    end
  end

  context 'raise an error' do
    it 'should raise an error that is not rateLimitExceeded' do
      error = 'An error!'
      allow(GoogleBigquery::Jobs).to receive(:query).and_raise(error)
      expect { step.run_with.to_a }.to raise_error(error)
    end
  end

  context 'retry querying' do
    it 'should retry querying if rateLimitExceeded is raised' do
      error = '[BigQuery: global]: rateLimitExceeded Exceeded rate limits:
        Your project exceeded quota for concurrent queries. For more information,
        see https://cloud.google.com/bigquery/troubleshooting-errors'

      call_count = 0
      allow(GoogleBigquery::Jobs).to receive(:query) do
        if call_count == 0
          call_count += 1
          raise(error)
        else
          query_response
        end
      end
      expect(step.run_with.to_a).to eq(results)
    end
  end
end
