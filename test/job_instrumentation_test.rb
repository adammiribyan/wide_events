require "test_helper"
require "active_job"

ActiveJob::Base.logger = Logger.new(nil)

class JobInstrumentationTest < ActiveJob::TestCase
  class BaseJob < ActiveJob::Base
    include WideEvent::JobInstrumentation
  end

  class ProbeJob < BaseJob
    cattr_accessor :seen_attrs

    def perform
      self.class.seen_attrs = WideEvent.attributes
    end
  end

  class BoomJob < BaseJob
    def perform
      raise ArgumentError, "job boom"
    end
  end

  setup do
    @sink = WideEvent::Sinks::Memory.new
    WideEvent.configure { |c| c.sink = @sink }
    ProbeJob.seen_attrs = nil
  end

  test "perform runs inside a wide event with job attributes and flushes to the sink" do
    ProbeJob.perform_now
    attrs = ProbeJob.seen_attrs
    assert_equal true, attrs["main"]
    assert_equal "JobInstrumentationTest::ProbeJob", attrs["job.class"]
    assert_equal ProbeJob.new.queue_name, attrs["job.queue"]
    assert_equal false, attrs["job.scheduled"]
    assert_kind_of Integer, attrs["uptime_sec"]
    assert_nil WideEvent.attributes, "store must be closed after perform"
    assert_equal 1, @sink.events.length
    assert_equal true, @sink.events.first["main"]
  end

  test "queue latency is recorded for enqueued jobs" do
    perform_enqueued_jobs { ProbeJob.perform_later }
    assert_operator ProbeJob.seen_attrs["job.queue_latency_ms"], :>=, 0
    assert ProbeJob.seen_attrs["job.enqueued_at"].present?
  end

  test "exceptions set error attributes, flush, and re-raise" do
    assert_raises(ArgumentError) { BoomJob.perform_now }
    captured = @sink.events.first
    assert_equal true, captured["error"]
    assert_equal "ArgumentError", captured["exception.type"]
    assert_nil captured["exception.slug"]
  end

  test "the scheduled job detector is consulted" do
    WideEvent.configure { |c| c.scheduled_job_detector = ->(_job) { true } }
    ProbeJob.perform_now
    assert_equal true, ProbeJob.seen_attrs["job.scheduled"]
  end

  test "a raising detector degrades to false" do
    WideEvent.configure { |c| c.scheduled_job_detector = ->(_job) { raise "boom" } }
    ProbeJob.perform_now
    assert_equal false, ProbeJob.seen_attrs["job.scheduled"]
  end

  test "performs without a wide event when disabled" do
    WideEvent.configure { |c| c.enabled = false }
    ProbeJob.perform_now
    assert_nil ProbeJob.seen_attrs
    assert_empty @sink.events
  end
end
