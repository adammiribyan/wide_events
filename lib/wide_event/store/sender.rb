require "json"
require "zlib"
require "securerandom"

module WideEvent
  module Store
    # Owns the per-process bounded queue, batching, retry loop, and shutdown
    # flush for delivering canonical event JSON to the store. Application
    # threads only ever call #enqueue, which appends to an in-memory array
    # under a mutex and never performs network I/O; a single worker thread
    # seals batches, gzips them, and calls Client#ingest.
    #
    # PID-aware: every enqueue compares Process.pid (or the injected
    # process_id) against the PID recorded at construction/last reset. A
    # mismatch (post-fork) discards the inherited queue and worker thread and
    # starts fresh in the child before admitting the new event.
    class Sender
      MAX_QUEUE_EVENTS = 1000
      MAX_QUEUE_BYTES = 8 * 1024 * 1024
      MAX_BATCH_EVENTS = 100
      MAX_BATCH_BYTES = 1 * 1024 * 1024
      MAX_COMPRESSED_BYTES = 512 * 1024
      FLUSH_INTERVAL = 1.0
      BASE_BACKOFF = 1.0
      MAX_BACKOFF = 30.0
      MAX_RETRY_AGE = 24 * 60 * 60
      WARNING_INTERVAL = 60.0
      SHUTDOWN_TIMEOUT = 2.0

      WireBatch = Struct.new(:batch_id, :json, :compressed, :drop_count, :loss_report_id,
                              :sealed_at, :attempts, keyword_init: true)

      def initialize(client:, service:, environment:,
                      max_queue_events: MAX_QUEUE_EVENTS, max_queue_bytes: MAX_QUEUE_BYTES,
                      max_batch_events: MAX_BATCH_EVENTS, max_batch_bytes: MAX_BATCH_BYTES,
                      max_compressed_bytes: MAX_COMPRESSED_BYTES,
                      flush_interval: FLUSH_INTERVAL, base_backoff: BASE_BACKOFF,
                      max_backoff: MAX_BACKOFF, max_retry_age: MAX_RETRY_AGE,
                      warning_interval: WARNING_INTERVAL,
                      clock: -> { Time.now }, uuid: -> { SecureRandom.uuid },
                      process_id: -> { Process.pid }, pid: nil, autostart: true)
        @client = client
        @service = service
        @environment = environment
        @max_queue_events = max_queue_events
        @max_queue_bytes = max_queue_bytes
        @max_batch_events = max_batch_events
        @max_batch_bytes = max_batch_bytes
        @max_compressed_bytes = max_compressed_bytes
        @flush_interval = flush_interval
        @base_backoff = base_backoff
        @max_backoff = max_backoff
        @max_retry_age = max_retry_age
        @warning_interval = warning_interval
        @clock = clock
        @uuid = uuid
        @process_id = process_id
        @autostart = autostart

        @mutex = Mutex.new
        @cv = ConditionVariable.new
        reset_state_locked
        @pid = pid || @process_id.call
        @thread = nil
        @shutdown = false
        @shutdown_deadline = nil

        start_worker if @autostart
      end

      # Appends one canonical event JSON string to the bounded queue. Never
      # performs network I/O and never raises into the caller.
      def enqueue(event_json)
        @mutex.synchronize do
          reset_for_pid_change_locked
          admit_locked(event_json)
        end
        @cv.broadcast
        true
      rescue StandardError => e
        WideEvent.handle_error(e, "store_sender_enqueue")
        false
      end

      def pending_drops
        @mutex.synchronize { @pending_drops }
      end

      # Performs exactly one send attempt of the current (or newly sealed)
      # batch, ignoring the retry backoff schedule. Test-only synchronous
      # hook; the worker thread drives normal delivery.
      def flush_once
        tick(force: true)
        nil
      end

      # Best-effort flush: keeps sending until nothing remains or the
      # timeout elapses, then stops the worker thread.
      def shutdown(timeout: SHUTDOWN_TIMEOUT)
        @mutex.synchronize do
          @shutdown = true
          @shutdown_deadline = monotonic_now + timeout
        end
        @cv.broadcast
        if @thread
          @thread.join(timeout + 0.5)
          @thread.kill if @thread.alive?
        end
        @thread = nil
        nil
      rescue StandardError => e
        WideEvent.handle_error(e, "store_sender_shutdown")
        nil
      end

      # Test-only introspection of the raw pending queue.
      def __queue_for_test
        @mutex.synchronize { @queue.dup }
      end

      private

      def reset_state_locked
        @queue = []
        @queue_bytes = 0
        @queue_started_at = nil
        @pending_drops = 0
        @loss_report_id = nil
        @last_drop_warning_at = nil
        @last_drift_warning_at = nil
        @in_flight = nil
        @overflow = []
        @next_attempt_at = nil
      end

      def reset_for_pid_change_locked
        current = @process_id.call
        return if current == @pid

        @pid = current
        reset_state_locked
        @thread = nil # inherited thread reference is not ours to manage post-fork
        start_worker if @autostart
      end

      def admit_locked(event_json)
        @queue_started_at = @clock.call if @queue.empty?
        bytes = event_json.bytesize
        @queue << event_json
        @queue_bytes += bytes
        enforce_bounds_locked
      end

      def enforce_bounds_locked
        while @queue.length > @max_queue_events || @queue_bytes > @max_queue_bytes
          dropped = @queue.shift
          break if dropped.nil?
          @queue_bytes -= dropped.bytesize
          record_drop_locked
        end
      end

      def record_drop_locked
        @pending_drops += 1
        @loss_report_id ||= @uuid.call
        warn_drop_locked
      end

      def warn_drop_locked
        now = @clock.call
        return if @last_drop_warning_at && (now - @last_drop_warning_at) < @warning_interval
        @last_drop_warning_at = now
        WideEvent.handle_error(
          StandardError.new("wide_events store sender queue is full: #{@pending_drops} event(s) dropped so far"),
          "store_sender_queue_overflow"
        )
      end

      # --- worker loop -------------------------------------------------

      def start_worker
        @thread = Thread.new { worker_loop }
        @thread.name = "wide_event.store.sender" if @thread.respond_to?(:name=)
        @thread.abort_on_exception = false
      end

      def worker_loop
        loop do
          shutting_down, deadline = @mutex.synchronize { [ @shutdown, @shutdown_deadline ] }
          if shutting_down
            break unless work_remaining?
            break if deadline && monotonic_now >= deadline
            # Still honor any pending retry backoff (clamped to the shutdown
            # deadline) instead of spinning: without this, a store that
            # answers instantly with a failure (e.g. a stopped local
            # sidecar giving immediate ECONNREFUSED) turns the drain into an
            # unthrottled busy loop that hammers the store and floods the
            # error handler for the whole shutdown window.
            wait_for_retry_during_shutdown(deadline)
            break if deadline && monotonic_now >= deadline
            tick(force: true)
            next
          end
          tick
          wait_for_more_work
        end
      rescue StandardError => e
        WideEvent.handle_error(e, "store_sender_worker")
      end

      def wait_for_retry_during_shutdown(deadline)
        @mutex.synchronize do
          return unless @in_flight && @next_attempt_at
          wait_until = deadline ? [ @next_attempt_at, deadline ].min : @next_attempt_at
          remaining = wait_until - monotonic_now
          @cv.wait(@mutex, remaining) if remaining.positive?
        end
      end

      def work_remaining?
        @mutex.synchronize { !(@queue.empty? && @in_flight.nil? && @overflow.empty?) }
      end

      def wait_for_more_work
        @mutex.synchronize do
          return if @shutdown
          timeout = next_wait_locked
          @cv.wait(@mutex, timeout)
        end
      end

      def next_wait_locked
        if @in_flight && @next_attempt_at
          remaining = @next_attempt_at - monotonic_now
          return remaining.clamp(0.01, @flush_interval) if remaining.positive?
        end
        if @queue_started_at && !@queue.empty?
          remaining = @flush_interval - (@clock.call - @queue_started_at)
          return remaining.clamp(0.01, @flush_interval) if remaining.positive?
        end
        @flush_interval
      end

      # --- send/seal cycle ----------------------------------------------

      def tick(force: false)
        batch = nil
        expired_batch = nil
        @mutex.synchronize do
          reset_for_pid_change_locked
          if @in_flight && batch_expired_locked?(@in_flight)
            expired_batch = @in_flight
            @in_flight = nil
          elsif !force && @in_flight && @next_attempt_at && monotonic_now < @next_attempt_at
            return nil
          else
            batch = next_batch_locked(force: force)
          end
        end
        if expired_batch
          WideEvent.handle_error(
            StandardError.new("batch exceeded the #{@max_retry_age}s retry horizon"),
            "store_sender_batch_expired"
          )
          return nil
        end
        return nil if batch.nil?
        respond_to_batch(batch)
        nil
      end

      def batch_expired_locked?(batch)
        (@clock.call - batch.sealed_at) > @max_retry_age
      end

      def next_batch_locked(force:)
        return @in_flight if @in_flight

        batch = @overflow.shift
        unless batch
          return nil unless force || seal_ready_locked?
          sealed = seal_locked
          return nil if sealed.empty?
          batch = sealed.shift
          @overflow.concat(sealed)
        end
        @in_flight = batch
      end

      # Only actually seal once the queue has reached the batch's size caps
      # or the oldest queued event has been waiting flush_interval seconds —
      # otherwise every enqueue's wakeup would immediately seal-and-send
      # that single event as its own request, defeating the batch protocol
      # under normal (sub-flush_interval) traffic. `force` (flush_once, and
      # the shutdown drain) bypasses this and seals whatever is queued.
      def seal_ready_locked?
        return false if @queue.empty?
        return true if @queue.length >= @max_batch_events
        return true if @queue_bytes >= @max_batch_bytes
        @queue_started_at && (@clock.call - @queue_started_at) >= @flush_interval
      end

      def seal_locked
        return [] if @queue.empty?

        events = []
        bytes = 0
        while !@queue.empty? && events.length < @max_batch_events
          candidate_bytes = @queue.first.bytesize
          break if events.any? && bytes + candidate_bytes > @max_batch_bytes
          events << @queue.shift
          bytes += candidate_bytes
          @queue_bytes -= candidate_bytes
        end
        @queue_started_at = @queue.empty? ? nil : @clock.call

        drop_count = @pending_drops
        loss_report_id = drop_count.positive? ? (@loss_report_id ||= @uuid.call) : nil
        split_into_wire_batches(events, drop_count, loss_report_id)
      end

      def split_into_wire_batches(events, drop_count, loss_report_id)
        batch = build_wire_batch(events, drop_count, loss_report_id)
        return [ batch ] if events.length <= 1 || batch.compressed.bytesize <= @max_compressed_bytes

        mid = events.length / 2
        split_into_wire_batches(events[0...mid], drop_count, loss_report_id) +
          split_into_wire_batches(events[mid..], 0, nil)
      end

      def build_wire_batch(events, drop_count, loss_report_id)
        envelope = {
          "protocol_version" => 1,
          "batch_id" => @uuid.call,
          "service" => @service,
          "environment" => @environment,
          "sent_at" => @clock.call.utc.iso8601(3),
          "dropped_events_since_last_batch" => drop_count,
          "loss_report_id" => loss_report_id,
          "events" => events.map { |event_json| JSON.parse(event_json) }
        }.compact
        json = JSON.generate(envelope)

        WireBatch.new(
          batch_id: envelope["batch_id"],
          json: json,
          compressed: Zlib.gzip(json),
          drop_count: drop_count,
          loss_report_id: loss_report_id,
          sealed_at: @clock.call,
          attempts: 0
        )
      end

      def respond_to_batch(batch)
        batch.attempts += 1
        ack = @client.ingest(batch.compressed)
        on_ack(batch, ack)
      rescue Client::Error => e
        e.retryable? ? on_retryable_error(batch, e) : on_permanent_error(batch, e)
      rescue StandardError => e
        on_retryable_error(batch, e)
      end

      def on_ack(batch, ack)
        warn_drift(ack.warnings) if ack.respond_to?(:warnings) && ack.warnings && !ack.warnings.empty?
        @mutex.synchronize do
          @in_flight = nil
          if batch.loss_report_id
            @pending_drops = [ @pending_drops - batch.drop_count, 0 ].max
            @loss_report_id = nil
          end
        end
        @cv.broadcast
      end

      def on_permanent_error(batch, error)
        WideEvent.handle_error(error, "store_sender_batch_rejected")
        @mutex.synchronize { @in_flight = nil }
        @cv.broadcast
      end

      # The 24-hour retry-age expiry itself is checked proactively at the
      # top of #tick (before a retry attempt is even made); this handler
      # only schedules the next backoff for a batch that is still young
      # enough to retry.
      def on_retryable_error(batch, error)
        WideEvent.handle_error(error, "store_sender_batch_retry")
        delay = backoff_delay(batch.attempts)
        @mutex.synchronize do
          @in_flight = batch
          @next_attempt_at = monotonic_now + delay
        end
      end

      def backoff_delay(attempts)
        # Clamp the exponent: a batch retried hundreds of times (e.g. during
        # a hard shutdown drain, which retries with no delay) would
        # otherwise compute 2**huge_number, a wasted bignum that still gets
        # thrown away by the [.., @max_backoff].min below.
        exponent = [ attempts - 1, 10 ].min
        capped = [ @base_backoff * (2**exponent), @max_backoff ].min
        capped * (0.5 + (rand * 0.5))
      end

      def warn_drift(warnings)
        report = @mutex.synchronize do
          now = @clock.call
          next false if @last_drift_warning_at && (now - @last_drift_warning_at) < @warning_interval
          @last_drift_warning_at = now
          true
        end
        return unless report
        WideEvent.handle_error(
          StandardError.new(warnings.first.to_s[0, 200]),
          "store_sender_ack_warning"
        )
      end

      def monotonic_now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
