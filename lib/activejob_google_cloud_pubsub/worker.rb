require 'active_job/base'
require 'active_support/core_ext/numeric/time'
require 'activejob_google_cloud_pubsub/pubsub_extension'
require 'google/cloud/pubsub'
require 'json'
require 'logger'

module ActiveJob
  module GoogleCloudPubsub
    class Worker
      using PubsubExtension

      def initialize(queue: 'default', pubsub: Google::Cloud::Pubsub.new(timeout: 60), logger: Logger.new($stdout))
        @queue_name  = queue
        @pubsub      = pubsub
        @logger      = logger
      end

      def run
        subscriber = @pubsub.subscription_for(@queue_name).listen(streams: 1, threads: { callback: 1 }) do |message|
          @logger&.info "Message(#{message.message_id}) was received."
          process message
        end

        subscriber.on_error do |error|
          @logger&.error(error)
        end

        @ack_deadline = subscriber.deadline

        subscriber.start

        sleep
      end

      def ensure_subscription
        @pubsub.subscription_for @queue_name

        nil
      end

      private

      def process(message)
        timer_opts = {
          # Extend ack deadline when only 10% of allowed time or 5 seconds are left, whichever comes first
          execution_interval: [(@ack_deadline * 0.9).round, @ack_deadline - 5].min.seconds,
          timeout_interval: 5.seconds,
          run_now: true
        }

        delay_timer = Concurrent::TimerTask.execute(timer_opts) do
          message.modify_ack_deadline! @ack_deadline
        end

        begin
          succeeded = false
          failed    = false

          ActiveJob::Base.execute JSON.parse(message.data)

          succeeded = true
        rescue StandardError
          failed = true
          raise
        ensure
          delay_timer.shutdown

          if succeeded || failed
            message.acknowledge!
            @logger&.info "Message(#{message.message_id}) was acknowledged."
          else
            # terminated from outside
            message.reject!
          end
        end
      end
    end
  end
end
