require "active_support/concern"

module WideEvent
  # One wide event per job execution: the job-shaped unit of work. Included
  # into ActiveJob::Base by the railtie; include it manually in ApplicationJob
  # when not using the railtie.
  #
  # Known semantics: exceptions later swallowed by retry_on/discard_on still
  # mark this execution error=true: the wide event records that THIS attempt
  # failed, which is accurate per-execution; roll up by job.id to judge the
  # job.
  module JobInstrumentation
    extend ActiveSupport::Concern

    included do
      around_perform :wide_event
    end

    private

    def wide_event(&block)
      return block.call unless WideEvent.enabled?

      WideEvent.with do |attrs|
        enqueued = enqueued_at.is_a?(String) ? Time.iso8601(enqueued_at) : enqueued_at
        WideEvent.set({
          "main" => true,
          "job.class" => self.class.name,
          "job.queue" => queue_name,
          "job.id" => job_id,
          "job.executions" => executions,
          "job.enqueued_at" => enqueued,
          "job.queue_latency_ms" => enqueued ? ((Time.now - enqueued) * 1000).round : nil,
          "job.scheduled" => wide_event_scheduled?
        }.merge(WideEvent.uptime_attributes))
        start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        begin
          block.call
        rescue Exception => e
          WideEvent.set("error" => true, "exception.type" => e.class.name,
                        "exception.message" => e.message.to_s[0, 500])
          raise
        ensure
          WideEvent.set("duration_ms" => ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) * 1000.0).round(2))
          WideEvent.flush(attrs)
        end
      end
    end

    def wide_event_scheduled?
      detector = WideEvent.config.scheduled_job_detector
      detector ? detector.call(self) == true : false
    rescue StandardError
      false
    end
  end
end
