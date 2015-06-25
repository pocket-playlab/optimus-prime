require 'excon'
require 'google/api_client'

module OptimusPrime
  module Destinations
    class GoogleCloudStorageBucket < Destination
      # This destination represents a single bucket in Google Cloud Storage, and uploads files
      # to that bucket.
      #
      # Upload Strategy:
      # The destination uploads files in parallel using threads, which means multiple files can
      # be uploaded at the same time.
      # For each file, the destination will make 4 back-offs if the upload didn't complete for
      # some reason. Besides, a `MAX_RETRIES` number of retries will be made if the upload failed
      # due to errors related to network.
      #
      # Input: a stream of hashes that contains the key `file` whose value is path to the file
      # to be uploaded in the local filesystem, and the name of the file after being uploaded.
      #
      # Output: a stream of hashes identical to the input stream, but with the value of `file`
      # replaced with a Google Cloud Storage Object Data object, which is returned by Google
      # Cloud Storage when the upload is finished.
      #
      # Parameters:
      # - `client_email` and `private_key` for authenticating with Google Cloud Platform API.
      # - `bucket`: the unique name of the bucket represented by this destination.
      # - `options` a hash of options, it can contain the following options:
      #   * `base_local_path`: if present, it will be prepended to file pathes when creating an
      #     `UploadIO` locally. It won't, however, be added to the remoe file names. defaults to
      #     `nil` (no base).
      #   * `content_type` content type of the files to be uploaded, defaults to `application/json`.
      #   * `upload_type`: the google cloud storage upload type to be used. Defaults to `resumable.
      #     see https://cloud.google.com/storage/docs/json_api/v1/how-tos/upload for details.

      attr_reader :client_email, :private_key, :bucket,
                  :base_local_path, :upload_type, :content_type
      MAX_RETRIES = 4

      def initialize(client_email:, private_key:, bucket:, options: {})
        Faraday.default_adapter = :excon
        @client_email = client_email
        @private_key  = OpenSSL::PKey::RSA.new(private_key)
        @bucket = bucket
        opts = default_options.merge(options)
        @base_local_path = opts[:base_local_path]
        @upload_type = opts[:upload_type]
        @content_type = opts[:content_type]
        @upload_jobs = []
      end

      def default_options
        { base_local_path: nil, content_type: 'application/json', upload_type: 'resumable' }
      end

      def write(record)
        @upload_jobs << Thread.new { upload(record) }
      end

      private

      def finish
        @upload_jobs.each(&:join)
      end

      def client
        @client ||= begin
          client = Google::APIClient.new application_name:    'Optimus Prime',
                                         application_version: OptimusPrime::VERSION,
                                         auto_refresh_token:  true
          scope = 'https://www.googleapis.com/auth/devstorage.read_write'
          asserter = Google::APIClient::JWTAsserter.new(client_email, scope, private_key)
          client.authorization = asserter.authorize
          client
        end
      end

      def gcs
        @gcs ||= client.discovered_api 'storage', 'v1'
      end

      def path_for(file)
        base_local_path ? File.join(base_local_path, file) : file
      end

      def upload(record)
        retries ||= 1
        duration ||= 1
        logger.info "Upload attempt ##{retries} for: #{record[:file]}"
        media = Google::APIClient::UploadIO.new(path_for(record[:file]), content_type)
        result = perform_upload(record[:file], media)
        push record.merge(file: result)
        logger.info "Success: file #{record[:file]} was uploaded as #{result.name}"
      rescue
        logger.error "Failure: attempt ##{retries} to upload #{record[:file]} failed."
        raise unless retries < MAX_RETRIES
        sleep duration *= 2
        retries += 1
        retry
      end

      def perform_upload(name, media)
        result = execute_upload(name, media)
        4.times do |time|
          result.resumable_upload.resumable? ? sleep((time + 1) * 2) : break
          result = client.execute(result.resumable_upload)
        end
        raise Exception("Failed uploading #{name}") unless result.resumable_upload.complete?
        result.data
      end

      def execute_upload(name, media)
        client.execute(
          api_method: gcs.objects.insert,
          parameters: { uploadType: upload_type, bucket: bucket, name: name },
          body_object: { contentType: content_type },
          media: media
        )
      end
    end
  end
end
