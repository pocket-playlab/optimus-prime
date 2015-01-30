require_relative '../optimus_init.rb'
require 'date'
require "net/http"
require "uri"
require 'csv'

class Appsflyer < OptimusPrime::Source
  attr_accessor :columns, :table_data, :api_token, :url, :data

  def initialize(columns, report_type, from_date: Date.today, to_date: Date.today)
    @columns = columns
    @url = "https://hq.appsflyer.com/export/id855124397/"
    @url += "#{report_type}?api_token=#{ENV['APPSFLYER_API_TOKEN']}"
    @url += "&from=#{from_date}"
    @url += "&to=#{to_date}"
    connect(@url, report_type)
    @table_data = Array.new
  end

  def columns
    return @columns
  end

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
        @data = response.body.force_encoding('utf-8')
        retrieve_data
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

  def retrieve_data
    @table_data = CSV.parse(@data)
    @table_data
  end
end
# ENV['APPSFLYER_API_TOKEN'] = 'a6e04203-565f-4d2c-9413-0d2b4dd04bf7'
# Appsflyer.new([], 'installs_report', from_date: '2015-01-19', to_date: '2015-01-22')