module FlurryHelpers
  def stub_flurry_request(url, params, *responses)
    responses = responses.map do |response|
      { status: response[:status] || 200, body: response[:body],
        headers: { 'content-type' => response[:content_type] || 'application/json' } }
    end
    stub_request(:get, "http://api.flurry.com/rawData/#{url}").with(query: params).to_return(responses)
  end

  def flurry_report(report_id)
    { apiAccessCode: 'SOMEFAKEAPIACCESSCODE', reportId: report_id }
  end

  def flurry_params
    { apiAccessCode: api_access_code, apiKey: api_key,
      startTime: '1422662400000', endTime: '1422748800000' }
  end

  def api_access_code
    @api_access_code ||= SecureRandom.hex
  end

  def api_key
    @api_key ||= SecureRandom.hex
  end

  def request_report_response
    File.read('spec/supports/flurry/request_report_response.json')
  end

  def events_file
    File.read 'spec/supports/flurry/events.json.gz'
  end
end

RSpec.configure do |config|
  config.include FlurryHelpers
end
