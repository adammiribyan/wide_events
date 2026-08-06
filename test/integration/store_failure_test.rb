require_relative "integration_helper"

# The failure half of the cross-runtime gate. Every failure here is injected
# for real - a hung-up TCP connection, a SIGTERMed process, a probe-driven
# disk floor - because the guarantees under test (idempotent retry, durable
# reopen, bounded loss reporting, read-during-degradation, socket-only
# backup) only hold if both runtimes actually behave that way on the wire.
class StoreFailureTest < StoreIntegrationTest
  test "a response lost after commit is retried and deduplicated into one row" do
    store = start_store
    proxy = start_proxy(target_port: store.port, drop_responses: 1)

    sender, sink = build_host("web-1", url: proxy.url, base_backoff: 0.1, max_backoff: 0.5)
    sink.flush(request_event(controller: "orders#create"))

    # First attempt: the store commits, the proxy eats the 200.
    sender.flush_once
    wait_until(message: "the proxy never dropped a response") { proxy.dropped == 1 }
    assert_equal 1, store.one_value("SELECT count(*) FROM wide_events"),
                 "the store must have committed the batch before the response was lost"

    # The sender retries the identical batch; the store recognizes the
    # batch_id and acknowledges it without inserting a second copy.
    sender.shutdown(timeout: SHUTDOWN_TIMEOUT)

    assert_equal 1, store.one_value("SELECT count(*) FROM wide_events")
    assert_equal 1, store.one_value("SELECT batch_ledger_rows FROM wide_events_store_status")
  end

  test "events committed before a restart are still queryable after the store reopens" do
    store = start_store
    sender, sink = build_host("web-1", url: store.url)
    3.times { |i| sink.flush(request_event(controller: "orders##{i}")) }
    sender.shutdown(timeout: SHUTDOWN_TIMEOUT)
    assert_equal 3, store.one_value("SELECT count(*) FROM wide_events")

    store.stop
    refute store.up?, "the store must actually be down"

    store.start
    assert_equal 3, store.one_value("SELECT count(*) FROM wide_events"),
                 "a clean checkpoint must survive process restart"

    reopened_sender, reopened_sink = build_host("web-2", url: store.url)
    reopened_sink.flush(request_event(controller: "orders#after-restart"))
    reopened_sender.shutdown(timeout: SHUTDOWN_TIMEOUT)

    assert_equal 4, store.one_value("SELECT count(*) FROM wide_events")
    assert_equal 1, store.one_value("SELECT count(*) FROM wide_events WHERE source = 'web-2'")
    assert_equal 2, store.one_value("SELECT count(DISTINCT source) FROM wide_events")
  end

  test "an outage overflows the bounded queue and the recovered store records the loss" do
    # The sender reports from its own worker thread, so guard the tally.
    lock = Mutex.new
    reported_errors = []
    WideEvent.configure do |config|
      config.error_handler = ->(_exception, message) { lock.synchronize { reported_errors << message } }
    end
    errors = -> { lock.synchronize { reported_errors.dup } }

    store = StoreIntegration::StoreProcess.new
    track_store(store)
    store.start
    url = store.url
    store.stop

    # A deliberately small bound so the overflow is quick and exact; the
    # store is genuinely unreachable for the whole enqueue window.
    sender, sink = build_host("web-1", url: url, max_queue_events: 25, base_backoff: 0.05, max_backoff: 0.2)
    60.times { |i| sink.flush(request_event(controller: "orders##{i}")) }

    wait_until(message: "the bounded queue never overflowed") { sender.pending_drops.positive? }
    wait_until(message: "the sender never hit the dead store on the wire") do
      errors.call.include?("store_sender_batch_retry")
    end
    # The tally is final here: drops only happen on enqueue (all done) and
    # only fall when a batch is acknowledged (the store is still down).
    drops = sender.pending_drops
    assert_operator drops, :>, 0
    assert_includes errors.call, "store_sender_queue_overflow"

    store.start
    sender.shutdown(timeout: SHUTDOWN_TIMEOUT)

    stored = store.one_value("SELECT count(*) FROM wide_events")
    assert_equal 60 - drops, stored, "everything the queue kept must arrive once the store is back"

    loss = store.query(
      "SELECT count(*) AS reports, coalesce(sum(dropped_events), 0)::BIGINT AS dropped FROM wide_events_loss_reports"
    )
    reports, reported = loss.rows.first
    assert_operator reports, :>=, 1, "the recovered store must have persisted a loss report"
    assert_operator reported, :>=, 1
    # Loss reports are deduplicated by id, so a report re-sent while more
    # drops accumulated records the count it was sealed with - never more
    # than the sender actually dropped.
    assert_operator reported, :<=, drops
    assert_equal reported, store.one_value("SELECT reported_dropped_events FROM wide_events_store_status")
  end

  test "a store below its disk floor rejects ingest with 507 while queries keep working" do
    store = start_store
    sender, sink = build_host("web-1", url: store.url)
    sink.flush(request_event(controller: "orders#index"))
    sender.shutdown(timeout: SHUTDOWN_TIMEOUT)
    assert_equal 1, store.one_value("SELECT count(*) FROM wide_events")

    # 1 GiB free on the probe's 50 GiB volume is below the 5 GiB write floor.
    store.restart(env: { "WIDE_EVENTS_TESTPROBE_FREE_BYTES" => (1024 * 1024 * 1024).to_s })

    client = store.client
    body = Zlib.gzip(JSON.generate(
      "protocol_version" => 1,
      "batch_id" => SecureRandom.uuid,
      "service" => "checkout",
      "environment" => "test",
      "dropped_events_since_last_batch" => 0,
      "events" => [ JSON.parse(WideEvent::Store::Envelope.build(
        request_event(controller: "orders#degraded"),
        clock: -> { Time.now }, uuid: -> { SecureRandom.uuid }, env: { "HOSTNAME" => "web-1" }
      )) ]
    ))

    error = assert_raises(WideEvent::Store::Client::Error) { client.ingest(body) }
    assert_match(/507/, error.message)
    assert error.retryable?, "507 must be retryable so the sender holds the batch instead of dropping it"

    # Reads are unaffected: this is exactly the state an operator has to be
    # able to diagnose from.
    assert_equal 1, store.one_value("SELECT count(*) FROM wide_events")
    degraded = store.query("SELECT write_ready, degradation_reason FROM wide_events_store_status")
    assert_equal [ false, "disk_space_low" ], degraded.rows.first

    # And /up still answers 200 with the reason, so Kamal's proxy healthcheck
    # keeps the container in rotation for reads.
    response = Net::HTTP.get_response(URI("#{store.url}/up"))
    assert_equal "200", response.code
    health = JSON.parse(response.body)
    assert_equal false, health["write_ready"]
    assert_equal "disk_space_low", health["degradation_reason"]
  end

  test "the admin CLI takes a verified backup over the private unix socket" do
    store = start_store
    sender, sink = build_host("web-1", url: store.url)
    2.times { sink.flush(request_event(controller: "orders#index")) }
    sender.shutdown(timeout: SHUTDOWN_TIMEOUT)

    assert_nil store.one_value("SELECT last_backup_at FROM wide_events_store_status")

    output, status = store.admin("backup")
    assert_equal 0, status, output
    path = output.strip
    assert File.file?(path), "expected a backup file at #{path.inspect} (output: #{output.inspect})"
    assert_operator File.size(path), :>, 0
    assert_equal File.realpath(File.join(store.directory, "backups")), File.dirname(path)
    assert_equal ".duckdb", File.extname(path)

    refute_nil store.one_value("SELECT last_backup_at FROM wide_events_store_status")
    assert_nil store.one_value("SELECT last_backup_error FROM wide_events_store_status")

    # The socket is the only way in: an admin client pointed elsewhere fails
    # rather than opening the database behind the running process's back.
    before = Dir.glob(File.join(store.directory, "**", "*")).sort
    missing = IO.popen(
      { "WIDE_EVENTS_ADMIN_SOCKET" => File.join(store.directory, "absent.sock") },
      [ StoreIntegration.binary, "admin", "backup" ],
      err: [ :child, :out ], &:read
    )
    refute_equal 0, $?.exitstatus, missing
    assert_equal before, Dir.glob(File.join(store.directory, "**", "*")).sort,
                 "a socketless admin client must not touch the database directory"
  end
end
