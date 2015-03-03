require 'spec_helper'
require 'optimus_prime/sources/bigquery'

describe OptimusPrime::Sources::Bigquery do
  describe '#each' do

    response_rows = [{'f'=> [{'v'=>nil}, {'v'=>'android'}, {'v'=>'0.0'}, {'v'=>'88550'}, {'v'=>'true'}]},
                     {'f'=> [{'v'=>'a'}, {'v'=>nil}, {'v'=>'4.2'}, {'v'=>'28200'}, {'v'=>'false'}]},
                     {'f'=> [{'v'=>'b'}, {'v'=>'android'}, {'v'=>nil}, {'v'=>'47325'}, {'v'=>'true'}]},
                     {'f'=> [{'v'=>'b'}, {'v'=>'android'}, {'v'=>'2.15'}, {'v'=>nil}, {'v'=>'false'}]},
                     {'f'=> [{'v'=>'b'}, {'v'=>'android'}, {'v'=>'42.9'}, {'v'=>'128175'}, {'v'=>nil}]}]

    project_id = 'project-id'
    job_id = 'job-id'

    query_response = {
      'kind'=>'bigquery#queryResponse',
      'schema'=>
      {'fields'=>
        [{'name'=>'Game', 'type'=>'STRING', 'mode'=>'NULLABLE'},
         {'name'=>'Platform', 'type'=>'STRING', 'mode'=>'NULLABLE'},
         {'name'=>'PercentComplete', 'type'=>'FLOAT', 'mode'=>'NULLABLE'},
         {'name'=>'MinScore', 'type'=>'INTEGER', 'mode'=>'NULLABLE'},
         {'name'=>'IsTester', 'type'=>'BOOLEAN', 'mode'=>'NULLABLE'}]},
      'jobReference'=>{'projectId'=>project_id, 'jobId'=>job_id},
      'totalRows'=>response_rows.count.to_s,
      'rows'=>response_rows,
      'totalBytesProcessed'=> '1200',
      'jobComplete'=> true,
      'cacheHit'=> false
    }

    results = [{ :Game => nil, :Platform => 'android', :PercentComplete => 0.0, :MinScore => 88550, :IsTester => true },
               { :Game => 'a', :Platform => nil, :PercentComplete => 4.2, :MinScore => 28200, :IsTester => false },
               { :Game => 'b', :Platform => 'android', :PercentComplete => nil, :MinScore => 47325, :IsTester => true },
               { :Game => 'b', :Platform => 'android', :PercentComplete => 2.15, :MinScore => nil, :IsTester => false },
               { :Game => 'b', :Platform => 'android', :PercentComplete => 42.9, :MinScore => 128175, :IsTester => nil }]

    sql = %{ SELECT Game, Platform, PercentComplete, MIN(Score) AS MinScore, IsTester
            FROM [dataset.table];
          }

    let(:source) do
      OptimusPrime::Sources::Bigquery.new project_id: project_id,
                                          sql: sql,
                                          pass_phrase: 'notasecret',
                                          key_file: 'test-privatekey.p12',
                                          email: 'test@developer.gserviceaccount.com'
    end

    before :each do
      allow(GoogleBigquery::Auth).to receive_message_chain(:new, :authorize).and_return(true)
    end

    context 'one page result' do
      it 'should yield all results' do
        allow(GoogleBigquery::Jobs).to receive(:query).and_return(query_response)
        rows = []
        source.each { |row| rows << row }
        expect(rows).to eq(results)
      end
    end

    def fake_response
      
    end

    context 'multiple pages result' do
      it 'should yield all results' do
        incomplete_query_response = query_response.select { |k, v| ['kind', 'jobReference', 'jobComplete'].include? k }
        incomplete_query_response['jobComplete'] = false
        allow(GoogleBigquery::Jobs).to receive(:query).and_return(incomplete_query_response)

        first_response = query_response.clone
        first_response['rows'] = response_rows.take 2
        first_response['pageToken'] = '2'
        allow(GoogleBigquery::Jobs).to receive(:getQueryResults).with(project_id, job_id, {})
                                                                .and_return(first_response)
        second_response = first_response.clone
        second_response['rows'] = response_rows[2,2]
        second_response['pageToken'] = '3'
        allow(GoogleBigquery::Jobs).to receive(:getQueryResults).with(project_id, job_id, { pageToken: first_response['pageToken'] })
                                                                .and_return(second_response)

        third_response = second_response.clone
        third_response['rows'] = [response_rows.last]
        third_response.delete 'pageToken'
        allow(GoogleBigquery::Jobs).to receive(:getQueryResults).with(project_id, job_id, { pageToken: second_response['pageToken'] })
                                                                .and_return(third_response)

        rows = []
        source.each { |row| rows << row }
        expect(rows).to eq(results)
      end
    end
  end
end