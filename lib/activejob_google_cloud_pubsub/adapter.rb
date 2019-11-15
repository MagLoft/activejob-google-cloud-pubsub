require 'activejob_google_cloud_pubsub/pubsub_extension'
require 'concurrent'
require 'google/cloud/pubsub'
require 'json'
require 'logger'

module ActiveJob
  module GoogleCloudPubsub
    class Adapter
      using PubsubExtension

      def initialize(async: true, pubsub: Google::Cloud::Pubsub.new(timeout: 60), logger: Logger.new($stdout))
        @executor = async ? :io : :immediate
        @pubsub   = pubsub
        @logger   = logger
      end

      def enqueue(job, attributes = {})
        Concurrent::Promise.new(executor: @executor) do
          @pubsub.topic_for(job.queue_name).publish JSON.dump(job.serialize), attributes
        end.rescue do |e|
          @logger&.error e
        end.execute
      end

      def enqueue_at(job, timestamp)
        enqueue job, timestamp: timestamp
      end
    end
  end
end

require 'active_job'

ActiveJob::QueueAdapters::GoogleCloudPubsubAdapter = ActiveJob::GoogleCloudPubsub::Adapter
