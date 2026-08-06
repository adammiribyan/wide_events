require "test_helper"
require "securerandom"
require "json"
require "zlib"

class StoreSenderTest < ActiveSupport::TestCase
  CLOCK_START = Time.utc(2026, 8, 5, 12)

  test "enqueue never calls the HTTP client" do
    sender = build_sender(client: null_client, autostart: false)
    assert sender.enqueue(event_json)
  end

  test "PID mismatch discards inherited state before enqueue" do
    sender = build_sender(client: null_client, autostart: false, pid: 100, process_id: -> { 200 })
    sender.enqueue(event_json("child"))
    assert_equal [ event_json("child") ], sender.__queue_for_test
  end

  test "PID mismatch is a no-op when the PID has not changed" do
    sender = build_sender(client: null_client, autostart: false, pid: 100, process_id: -> { 100 })
    sender.enqueue(event_json("a"))
    sender.enqueue(event_json("b"))
    assert_equal [ event_json("a"), event_json("b") ], sender.__queue_for_test
  end

  test "drops the oldest events, oldest first, once the event-count bound is exceeded" do
    sender = build_sender(client: null_client, autostart: false, max_queue_events: 3, max_queue_bytes: 1_000_000)
    4.times { |i| sender.enqueue(event_json("e#{i}")) }
    assert_equal [ event_json("e1"), event_json("e2"), event_json("e3") ], sender.__queue_for_test
    assert_equal 1, sender.pending_drops
  end

  test "drops the oldest events once the byte-size bound is exceeded" do
    big = event_json("x" * 100)
    sender = build_sender(client: null_client, autostart: false, max_queue_events: 1_000, max_queue_bytes: big.bytesize * 2)
    3.times { sender.enqueue(big) }
    assert_equal 2, sender.__queue_for_test.length
    assert_equal 1, sender.pending_drops
  end

  test "rate-limits the drop warning to once per warning interval" do
    now = CLOCK_START
    clock = -> { now }
    errors = []
    WideEvent.configure { |c| c.error_handler = ->(e, msg) { errors << msg } }

    sender = build_sender(client: null_client, autostart: false, max_queue_events: 5,
                           max_queue_bytes: 1_000_000, warning_interval: 60.0, clock: clock)
    7.times { |i| sender.enqueue(event_json("e#{i}")) } # fills to 5, then drops e0 and e1 at the same instant
    assert_equal 2, sender.pending_drops
    assert_equal 1, errors.count { |m| m == "store_sender_queue_overflow" }

    now += 30 # still within the warning interval
    sender.enqueue(event_json("e7")) # another overflow drop, but suppressed
    assert_equal 3, sender.pending_drops
    assert_equal 1, errors.count { |m| m == "store_sender_queue_overflow" }

    now += 31 # 61s after the first warning: interval has elapsed
    sender.enqueue(event_json("e8"))
    assert_equal 4, sender.pending_drops
    assert_equal 2, errors.count { |m| m == "store_sender_queue_overflow" }
  end

  test "loss count remains pending until acknowledgement" do
    sender = build_sender(client: scripted_client(:timeout, :permanent_failure, :ok), autostart: false,
                           max_queue_events: 4, max_batch_events: 2)
    overflow_queue(sender, capacity: 4, by: 3)
    assert_equal 3, sender.pending_drops

    sender.flush_once # timeout on batch A: kept in-flight, unacknowledged
    assert_equal 3, sender.pending_drops

    sender.flush_once # permanent failure on batch A: discarded, snapshot still pending
    assert_equal 3, sender.pending_drops

    sender.flush_once # batch B (carries the same snapshot) succeeds
    assert_equal 0, sender.pending_drops
  end

  test "drops accumulated while a batch is in flight remain pending after that batch's ack" do
    sender = build_sender(client: scripted_client(:timeout, :ok), autostart: false,
                           max_queue_events: 4, max_batch_events: 2)
    overflow_queue(sender, capacity: 4, by: 2) # pending_drops == 2, queue has 4 events

    sender.flush_once # seals batch A carrying the snapshot (2); :timeout keeps it in-flight
    assert_equal 2, sender.pending_drops

    # More drops happen while batch A is still unacknowledged.
    sender.enqueue(event_json("new-1"))
    sender.enqueue(event_json("new-2"))
    sender.enqueue(event_json("new-3"))
    assert_equal 3, sender.pending_drops

    sender.flush_once # retries batch A (still carrying the ORIGINAL snapshot of 2); :ok acks it
    assert_equal 1, sender.pending_drops # only the snapshot (2) was subtracted, not all 3
  end

  test "a batch older than the retry horizon is discarded instead of retried forever" do
    now = CLOCK_START
    clock = -> { now }
    errors = []
    WideEvent.configure { |c| c.error_handler = ->(e, msg) { errors << msg } }
    client = scripted_client(:timeout) # exactly one scripted response: a second call fails the test
    sender = build_sender(client: client, autostart: false,
                           max_queue_events: 10, max_batch_events: 10, clock: clock, max_retry_age: 60)
    sender.enqueue(event_json)
    sender.flush_once # seals + fails once, batch kept in-flight
    assert_equal 1, client.calls

    now += 61 # past the retry horizon
    sender.flush_once # discovers the batch has expired: discards it without calling the client again

    assert_equal 1, client.calls, "the expired batch must not trigger another send attempt"
    assert_equal 1, errors.count { |m| m == "store_sender_batch_expired" }
    assert_empty sender.__queue_for_test
  end

  test "splits a batch into multiple compressed bodies when gzip would exceed the compressed cap" do
    recorder = recording_client
    sender = build_sender(client: recorder, autostart: false, max_queue_events: 100,
                           max_batch_events: 100, max_batch_bytes: 1_000_000, max_compressed_bytes: 450)
    # High-entropy attribute values so the batch doesn't compress away the
    # size a real production payload would have.
    10.times { sender.enqueue(event_json(SecureRandom.hex(60))) }

    20.times { sender.flush_once } # enough attempts to drain every split piece
    assert_operator recorder.calls.length, :>, 1, "expected the oversized batch to be split into more than one request"
    recorder.calls.each do |gzip_body|
      assert_operator gzip_body.bytesize, :<=, 450
    end
    assert_empty sender.__queue_for_test
    assert_equal 0, sender.pending_drops
  end

  # The sealing loop budgets event bytes only, but the store measures the
  # entire canonical batch document - protocol_version, ids, service,
  # environment, the loss report, the commas between events - against the very
  # same 1 MiB cap. A batch that overshoots there is answered 400, which is a
  # permanent error: the sender drops all hundred events and reports nothing,
  # because the loss counters only cover what the queue itself shed.
  test "a sealed batch's whole wire document fits the byte cap the store enforces" do
    recorder = recording_client
    sender = build_sender(client: recorder, autostart: false,
                           max_queue_events: 1000, max_queue_bytes: 16 * 1024 * 1024,
                           max_compressed_bytes: 64 * 1024 * 1024)

    # Queue events whose bytes sum to *exactly* the cap, so an accounting that
    # ignores the envelope produces a document one envelope over the line.
    limit = WideEvent::Store::Sender::MAX_BATCH_BYTES
    overhead = event_json("").bytesize
    sized = ->(bytes) { event_json("x" * (bytes - overhead)) }
    20.times { sender.enqueue(sized.call(50_000)) }
    sender.enqueue(sized.call(limit - 20 * 50_000))

    sender.flush_once
    assert_equal 1, recorder.calls.length
    wire = Zlib.gunzip(recorder.calls.first)

    assert_operator wire.bytesize, :<=, limit,
                    "the store charges this entire document, envelope included, against the same cap"
    assert_operator wire.bytesize, :>, limit - 60_000,
                    "the envelope reserve must not be so large that batches stop filling"
  end

  test "shutdown drains the queue within the timeout and stops the worker thread" do
    sender = build_sender(client: scripted_client(:ok, :ok, :ok), max_queue_events: 10, max_batch_events: 1)
    3.times { |i| sender.enqueue(event_json("e#{i}")) }
    sender.shutdown(timeout: 2.0)
    assert_empty sender.__queue_for_test
  end

  test "shutdown does not block past its timeout when delivery keeps failing" do
    sender = build_sender(client: always_timeout_client, max_queue_events: 10, max_batch_events: 1)
    sender.enqueue(event_json)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    sender.shutdown(timeout: 0.3)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    assert_operator elapsed, :<, 2.0
  end

  test "shutdown honors the retry backoff instead of busy-spinning against a store that fails instantly" do
    client = counting_timeout_client
    # A store that answers instantly with a retryable failure (e.g. a
    # stopped local sidecar giving immediate ECONNREFUSED) must not turn
    # the shutdown drain into an unthrottled loop: without the backoff-aware
    # wait, this ran well over a million attempts in a 2-second window.
    sender = build_sender(client: client, max_queue_events: 10, max_batch_events: 1,
                           base_backoff: 0.05, max_backoff: 0.2)
    sender.enqueue(event_json)
    sender.shutdown(timeout: 0.5)
    assert_operator client.calls.length, :<, 100,
      "expected a handful of backed-off attempts, not a busy-spin (#{client.calls.length} calls)"
  end

  test "events enqueued within the flush interval are batched into a single request, not one per event" do
    recorder = recording_client
    sender = build_sender(client: recorder, max_queue_events: 100, max_batch_events: 100, flush_interval: 0.3)
    5.times do |i|
      sender.enqueue(event_json("e#{i}"))
      sleep 0.02
    end
    sleep 0.5 # let flush_interval (0.3s) elapse so the worker seals the accumulated batch as one
    sender.shutdown(timeout: 2.0)

    assert_equal 1, recorder.calls.length, "5 events well inside the flush interval should be one request"
    sent_events = JSON.parse(Zlib.gunzip(recorder.calls.first))["events"]
    assert_equal 5, sent_events.length
  end

  test "an ack warning is reported through handle_error, rate-limited" do
    now = CLOCK_START
    clock = -> { now }
    errors = []
    WideEvent.configure { |c| c.error_handler = ->(e, msg) { errors << msg } }
    client = scripted_client_with_warnings([ "client service/environment differs from store configuration" ], [ "client service/environment differs from store configuration" ])
    sender = build_sender(client: client, autostart: false, max_queue_events: 10, max_batch_events: 1, clock: clock)

    sender.enqueue(event_json("a"))
    sender.flush_once
    assert_equal 1, errors.count { |m| m == "store_sender_ack_warning" }

    sender.enqueue(event_json("b"))
    sender.flush_once # same warning again, within the interval: suppressed
    assert_equal 1, errors.count { |m| m == "store_sender_ack_warning" }
  end

  test "an unexpected error from the client is treated as retryable and never raises out of enqueue or flush_once" do
    sender = build_sender(client: raising_client, autostart: false, max_queue_events: 10, max_batch_events: 1)
    sender.enqueue(event_json)
    assert_nothing_raised { sender.flush_once }
  end

  private

  # Deterministic (fixed clock + fixed uuid) so two calls with the same
  # label produce byte-identical strings, which matters for the
  # __queue_for_test equality assertions below.
  def event_json(label = "e")
    WideEvent::Store::Envelope.build(
      { "duration_ms" => 1.0, "label" => label },
      clock: -> { CLOCK_START },
      uuid: -> { "018f6f50-2f9d-7fb0-a258-6d9f95f20c01" },
      env: {}
    )
  end

  def build_sender(client:, service: "checkout", environment: "test", autostart: true, **opts)
    WideEvent::Store::Sender.new(client: client, service: service, environment: environment, autostart: autostart, **opts)
  end

  def overflow_queue(sender, capacity:, by:)
    (capacity + by).times { |i| sender.enqueue(event_json("seed-#{i}")) }
  end

  def null_client
    Object.new.tap { |c| c.define_singleton_method(:ingest) { |_body| raise "null_client should never be called" } }
  end

  def raising_client
    Object.new.tap { |c| c.define_singleton_method(:ingest) { |_body| raise "boom" } }
  end

  def always_timeout_client
    Object.new.tap do |c|
      c.define_singleton_method(:ingest) { |_body| raise WideEvent::Store::Client::Error.new("timeout", retryable: true) }
    end
  end

  def counting_timeout_client
    Object.new.tap do |c|
      calls = []
      c.define_singleton_method(:calls) { calls }
      c.define_singleton_method(:ingest) do |body|
        calls << body
        raise WideEvent::Store::Client::Error.new("timeout", retryable: true)
      end
    end
  end

  # `calls` lets tests assert exactly how many times the client was hit
  # (e.g. "the client must NOT be called again once a batch expires").
  # Running past the end of the script is a test-authoring bug, not an
  # implicit :ok — it raises loudly instead of silently succeeding, so a
  # missing/extra call fails the test instead of passing vacuously.
  def scripted_client(*responses)
    queue = responses.dup
    calls = 0
    Object.new.tap do |c|
      c.define_singleton_method(:calls) { calls }
      c.define_singleton_method(:ingest) do |_body|
        calls += 1
        raise "scripted_client called more times (#{calls}) than scripted (#{responses.length})" if queue.empty?
        script = queue.shift
        case script
        when :timeout
          raise WideEvent::Store::Client::Error.new("timeout", retryable: true)
        when :permanent_failure
          raise WideEvent::Store::Client::Error.new("bad request", retryable: false)
        when :ok
          WideEvent::Store::Client::Ack.new(protocol_version: 1, batch_id: SecureRandom.uuid, duplicate: false, warnings: [])
        end
      end
    end
  end

  def scripted_client_with_warnings(*warning_lists)
    queue = warning_lists.dup
    Object.new.tap do |c|
      c.define_singleton_method(:ingest) do |_body|
        warnings = queue.shift || []
        WideEvent::Store::Client::Ack.new(protocol_version: 1, batch_id: SecureRandom.uuid, duplicate: false, warnings: warnings)
      end
    end
  end

  def recording_client
    Object.new.tap do |c|
      calls = []
      c.define_singleton_method(:calls) { calls }
      c.define_singleton_method(:ingest) do |body|
        calls << body
        WideEvent::Store::Client::Ack.new(protocol_version: 1, batch_id: SecureRandom.uuid, duplicate: false, warnings: [])
      end
    end
  end
end
