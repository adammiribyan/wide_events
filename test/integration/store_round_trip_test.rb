require_relative "integration_helper"

# Three independent Ruby senders (two web hosts, one job host) against one
# real store process: the cross-host protocol claim the whole design rests
# on. Every event here is built by WideEvent::Store::Envelope, batched and
# gzipped by WideEvent::Store::Sender, decoded by store/internal/protocol,
# and read back through POST /v1/query.
class StoreRoundTripTest < StoreIntegrationTest
  WEB_EVENTS = 40
  JOB_EVENTS = 25

  test "two web sources and one job source land in one queryable table with no duplicates" do
    store = start_store
    _web1_sender, web1 = build_host("web-1", url: store.url)
    _web2_sender, web2 = build_host("web-2", url: store.url)
    _jobs_sender, jobs = build_host("jobs-1", url: store.url)

    threads = [
      Thread.new do
        WEB_EVENTS.times { |i| web1.flush(request_event(controller: "orders##{i % 4}")) }
      end,
      Thread.new do
        WEB_EVENTS.times { |i| web2.flush(request_event(controller: "orders##{i % 4}", status: i.even? ? 200 : 422)) }
      end,
      Thread.new do
        JOB_EVENTS.times { |i| jobs.flush(job_event(job_class: "ExportJob", queue: i.even? ? "default" : "low")) }
      end
    ]
    threads.each(&:join)

    @senders.each { |sender| sender.shutdown(timeout: SHUTDOWN_TIMEOUT) }

    total = WEB_EVENTS + WEB_EVENTS + JOB_EVENTS
    assert_equal total, store.one_value("SELECT count(*) FROM wide_events")
    assert_equal total, store.one_value("SELECT count(DISTINCT id) FROM wide_events"),
                 "every delivered event must be stored exactly once"

    by_source = store.query(<<~SQL)
      SELECT source, kind, count(*) AS events
      FROM wide_events
      GROUP BY source, kind
      ORDER BY source
    SQL
    assert_equal [ "source", "kind", "events" ], by_source.columns
    assert_equal [
      [ "jobs-1", "job", JOB_EVENTS ],
      [ "web-1", "request", WEB_EVENTS ],
      [ "web-2", "request", WEB_EVENTS ]
    ], by_source.rows

    # The store's own projected columns, not just the attributes blob: these
    # are what the documented queries and the debugging skill select.
    projected = store.query(<<~SQL)
      SELECT route, count(*) AS requests, count(status) AS with_status
      FROM wide_events
      WHERE kind = 'request'
      GROUP BY route
      ORDER BY route
    SQL
    assert_equal %w[orders#0 orders#1 orders#2 orders#3], projected.rows.map(&:first)
    assert_equal 2 * WEB_EVENTS, projected.rows.sum { |row| row[1] }
    assert_equal 2 * WEB_EVENTS, projected.rows.sum { |row| row[2] }

    assert_equal JOB_EVENTS, store.one_value("SELECT count(*) FROM wide_events WHERE job_class = 'ExportJob'")
    assert_equal 0, store.one_value("SELECT count(*) FROM wide_events WHERE duration_ms IS NULL")
    # Asserted positively: `service <> 'checkout'` is UNKNOWN for a NULL and
    # would count zero rows whether tenancy was stamped or not.
    assert_equal total,
                 store.one_value("SELECT count(*) FROM wide_events WHERE service = 'checkout' AND environment = 'test'")

    # The attributes VARIANT survives the round trip with its original types.
    assert_equal true, store.one_value("SELECT attributes.main::BOOLEAN FROM wide_events WHERE kind = 'request' LIMIT 1")

    status = store.query(<<~SQL)
      SELECT protocol_version, write_ready, rows_last_hour, store_version
      FROM wide_events_store_status
    SQL
    assert_equal [ 1, true, total, "0.0.0-integration" ], status.rows.first
  end

  # The client and the server must charge the same bytes for the same event.
  # Ruby measures its own JSON.generate output against 64 KiB; the store
  # re-encodes the decoded event and measures that. While the store's encoder
  # HTML-escaped, every & and < cost one byte on the client and six on the
  # server, so an event the client had just measured as legal came back 400 -
  # a permanent error that makes the sender drop the whole batch, with no loss
  # report and nothing in the data to show for it. This pins the seam shut at
  # the exact boundary, with the characters that used to inflate.
  test "an event sized to the client's limit with an &-and-< dense payload is accepted whole" do
    store = start_store
    sender, sink = build_host("web-1", url: store.url)

    base = request_event(controller: "search#index")
    sized = lambda do |payload|
      WideEvent::Store::Envelope.build(
        base.merge("http.query" => payload),
        clock: -> { Time.now }, uuid: -> { SecureRandom.uuid }, env: { "HOSTNAME" => "web-1" }
      ).bytesize
    end

    room = WideEvent::Store::Envelope::MAX_EVENT_BYTES - sized.call("")
    payload = "&<" * (room / 2)
    payload += "&" if room.odd?
    assert_equal WideEvent::Store::Envelope::MAX_EVENT_BYTES, sized.call(payload),
                 "the event must sit exactly on the client's limit for this to test the boundary"

    sink.flush(base.merge("http.query" => payload))
    sender.shutdown(timeout: SHUTDOWN_TIMEOUT)

    assert_equal 1, store.one_value("SELECT count(*) FROM wide_events"),
                 "a maximum-size event dense with & and < must be accepted, not 400ed away"
    assert_equal payload,
                 store.one_value("SELECT attributes['http.query']::VARCHAR FROM wide_events"),
                 "the payload must survive both runtimes' JSON encoders byte for byte"
  end

  test "an already-committed batch id is acknowledged as a duplicate instead of stored twice" do
    store = start_store
    client = store.client
    sender, sink = build_host("web-1", url: store.url)
    sink.flush(request_event(controller: "orders#show"))
    sender.shutdown(timeout: SHUTDOWN_TIMEOUT)

    assert_equal 1, store.one_value("SELECT count(*) FROM wide_events")
    assert_equal 1, store.one_value("SELECT batch_ledger_rows FROM wide_events_store_status")

    # Replay the exact bytes of a batch the store already committed.
    body = Zlib.gzip(JSON.generate(
      "protocol_version" => 1,
      "batch_id" => SecureRandom.uuid,
      "service" => "checkout",
      "environment" => "test",
      "dropped_events_since_last_batch" => 0,
      "sent_at" => Time.now.utc.iso8601(3),
      "events" => [ JSON.parse(WideEvent::Store::Envelope.build(
        request_event(controller: "orders#index"),
        clock: -> { Time.now }, uuid: -> { SecureRandom.uuid }, env: { "HOSTNAME" => "web-9" }
      )) ]
    ))

    first = client.ingest(body)
    second = client.ingest(body)
    refute first.duplicate
    assert second.duplicate, "a replayed batch_id must be acknowledged, not re-stored"
    assert_equal first.batch_id, second.batch_id
    assert_equal 2, store.one_value("SELECT count(*) FROM wide_events")
  end
end
