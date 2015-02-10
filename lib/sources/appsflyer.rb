require_relative '../optimus_init.rb'
require 'date'
require "net/http"
require "uri"
require 'csv'

class Appsflyer < OptimusPrime::Source
  attr_accessor :columns, :api_token, :response_data

  def initialize(columns, report_type, app_id, from_date: Date.today, to_date: Date.today)
    raise 'columns, report_type and app_id are required' unless columns && report_type && app_id
    super columns
    url = "https://hq.appsflyer.com/export/#{app_id}/"
    url += "#{report_type}?api_token=#{ENV['APPSFLYER_API_TOKEN']}"
    url += "&from=#{from_date}"
    url += "&to=#{to_date}"

    connect(url)
  end

  protected

  def implement_get_data
    @data = CSV.parse(@response_data)
  end

  private

  def connect(location, limit = 10, ssl: true)
    raise 'too many HTTP redirects' if limit == 0
    begin
      uri = URI(location)
      puts "connecting to #{location}"
      https = Net::HTTP.new(uri.host, uri.port)
      https.use_ssl = ssl
      request = Net::HTTP::Get.new(uri.request_uri)
      response = https.request(request)
      if response.code == '200'
        puts 'success!'
        @response_data = response.body.force_encoding('utf-8')
        get_data
      elsif response.code == '302'
        puts "redirect to #{response['location']}"
        connect(response['location'], 9)
      else
        puts 'error'
        puts response.code
      end
    rescue Exception => e
      puts e.message
    end
  end
end
