require 'bricolage/datasource'
require 'securerandom'
require 'aws-sdk-sqs'
require 'json'
require 'time'

module Bricolage

  class SQSDataSource < DataSource

    declare_type 'sqs'

    def initialize(region:, url:, access_key_id: nil, secret_access_key: nil,
        visibility_timeout:, max_number_of_messages: 10, wait_time_seconds: 20, noop: false)
      @region = region
      @url = url
      @access_key_id = access_key_id
      @secret_access_key = secret_access_key
      @visibility_timeout = visibility_timeout
      @max_number_of_messages = max_number_of_messages
      @wait_time_seconds = wait_time_seconds
      @noop = noop
    end

    attr_reader :region
    attr_reader :url
    attr_reader :access_key_id
    attr_reader :secret_access_key

    attr_reader :visibility_timeout
    attr_reader :max_number_of_messages
    attr_reader :wait_time_seconds

    def client
      @client ||= begin
        c = @noop ? DummySQSClient.new : Aws::SQS::Client.new(region: @region, access_key_id: @access_key_id, secret_access_key: @secret_access_key)
        SQSClientWrapper.new(c, logger: logger)
      end
    end

    #
    # High-Level Polling Interface
    #

    def handle_messages(handler:, message_class:)
      trap_signals
      polling_loop do
        result = poll or next true
        msgs = message_class.for_sqs_result(result)
        msgs.each do |msg|
          handler.handle(msg)
        end
        handler.after_message_batch
        break if terminating?
        msgs.empty?
      end
    end

    def trap_signals
      # Allows graceful stop
      Signal.trap(:TERM) {
        initiate_terminate
      }
      Signal.trap(:INT) {
        initiate_terminate
      }
    end
    private :trap_signals

    def initiate_terminate
      # No I/O allowed in this method
      @terminating = true
    end

    def terminating?
      @terminating
    end

    def polling_loop
      n_failure = 0
      while true
        failed = yield
        if failed
          n_failure += 1
        else
          n_failure = 0
        end
        insert_handler_wait(n_failure)
      end
    end
    private :polling_loop

    def insert_handler_wait(n_failure)
      sec = 2 ** [n_failure, 6].min   # max 64s
      logger.info "queue wait: sleep #{sec}" if n_failure > 0
      sleep sec
    end
    private :insert_handler_wait

    def poll
      result = receive_messages()
      unless result and result.successful?
        logger.error "ReceiveMessage failed: #{result ? result.error.message : '(result=nil)'}"
        return nil
      end
      logger.info "receive #{result.messages.size} messages"
      result
    end

    class MessageHandler
      # abstract logger()

      def handle(msg)
        logger.debug "handling message: #{msg.inspect}" if logger.debug?
        if handleable?(msg)
          call_handler_method(msg)
        else
          handle_unknown(msg)
        end
      end

      def handleable?(msg)
        respond_to?(handler_method(msg), true)
      end

      def call_handler_method(msg)
        __send__(handler_method(msg), msg)
      end

      def handler_method(msg)
        "handle_#{msg.message_type}".intern
      end

      # Unknown message handler.
      # Feel free to override this method.
      def handle_unknown(msg)
        # just ignore unknown message to make app migration easy
        logger.error "unknown message type: #{msg.message_type.inspect} (message-id: #{msg.message_id})"
      end

      # Called after each message batch (ReceiveMessage) is processed.
      # Override this method in subclasses on demand.
      def after_message_batch
      end
    end

    #
    # API-Level Interface
    #

    def receive_messages
      result = client.receive_message(
        queue_url: @url,
        max_number_of_messages: @max_number_of_messages,
        visibility_timeout: @visibility_timeout,
        wait_time_seconds: @wait_time_seconds
      )
      result
    end

    def put(msg)
      send_message(msg)
    end

    def send_message(msg)
      client.send_message(
        queue_url: @url,
        message_body: { 'Records' => [msg.body] }.to_json,
        delay_seconds: msg.delay_seconds
      )
    end

    def send_object(obj)
      send_message(ObjectMessage.new(obj))
    end

    def send_event(name, source: SQSMessage::SQS_EVENT_SOURCE, time: Time.now, **attrs)
      attrs['eventName'] = name
      attrs['eventSource'] = source
      attrs['eventTime'] = time.iso8601
      send_object(attrs)
    end

    def delete_message(msg)
      client.delete_message(
        queue_url: @url,
        receipt_handle: msg.receipt_handle
      )
    end

    def delete_message_async(msg)
      delete_message_buffer.put(msg)
    end

    def process_async_delete(now = Time.now)
      delete_message_buffer.flush(now)
    end

    def process_async_delete_force
      delete_message_buffer.flush_force
    end

    private

    def delete_message_buffer
      @delete_message_buffer ||= DeleteMessageBuffer.new(client, @url, logger)
    end

    class DeleteMessageBuffer

      BATCH_SIZE_MAX = 10   # SQS system limit
      MAX_RETRY_COUNT = 3

      def initialize(sqs_client, url, logger)
        @sqs_client = sqs_client
        @url = url
        @logger = logger
        @buf = {}
      end

      def put(msg)
        ent = Entry.new(msg)
        @buf[ent.id] = ent
        flush if full?
      end

      def empty?
        @buf.empty?
      end

      def full?
        @buf.size >= BATCH_SIZE_MAX
      end

      def size
        @buf.size
      end

      # Flushes all delayed delete requests, including pending requests
      def flush_force
        # retry continues in only 2m, now+1h must be after than all @next_issue_time
        flush(Time.now + 3600)
      end

      def flush(now = Time.now)
        entries = @buf.values.select {|ent| ent.issuable?(now) }
        return if entries.empty?
        @logger.info "flushing async delete requests"
        entries.each_slice(BATCH_SIZE_MAX) do |ents|
          res = @sqs_client.delete_message_batch(queue_url: @url, entries: ents.map(&:request_params))
          @logger.info "DeleteMessageBatch executed: #{res.successful.size} succeeded, #{res.failed.size} failed"
          issued_time = Time.now
          res.successful.each do |s|
            @buf.delete s.id
          end
          res.failed.each do |f|
            ent = @buf[f.id]
            unless ent
              @logger.error "[BUG] no corrensponding DeleteMessageBuffer entry: id=#{f.id}"
              next
            end
            ent.failed!(issued_time)
            if ent.too_many_failure?
              @logger.warn "DeleteMessage failure count exceeded the limit; give up: message_id=#{ent.message.message_id}, receipt_handle=#{ent.message.receipt_handle}"
              @buf.delete f.id
              next
            end
            @logger.info "DeleteMessageBatch partially failed (#{ent.n_failure} times): sender_fault=#{f.sender_fault}, code=#{f.code}, message=#{f.message}"
          end
        end
      end

      class Entry
        def initialize(msg)
          @message = msg
          @id = SecureRandom.uuid
          @n_failure = 0
          @last_issued_time = nil
          @next_issue_time = nil
        end

        attr_reader :id
        attr_reader :message
        attr_reader :n_failure

        def issuable?(now)
          @n_failure == 0 or now > @next_issue_time
        end

        def failed!(issued_time = Time.now)
          @n_failure += 1
          @last_issued_time = issued_time
          @next_issue_time = @last_issued_time + next_retry_interval
        end

        def next_retry_interval
          # 16s, 32s, 64s -> total 2m
          2 ** (3 + @n_failure)
        end

        def too_many_failure?
          # (first request) + (3 retry requests) = (4 requests)
          @n_failure > MAX_RETRY_COUNT
        end

        def request_params
          { id: @id, receipt_handle: @message.receipt_handle }
        end
      end

    end # class DeleteMessageBuffer

  end # class SQSDataSource


  class SQSClientWrapper

    def initialize(sqs, logger:)
      @sqs = sqs
      @logger = logger
    end

    def receive_message(**args)
      @logger.debug "receive_message(#{args.inspect})"
      @sqs.receive_message(**args)
    end

    def send_message(**args)
      @logger.debug "send_message(#{args.inspect})"
      @sqs.send_message(**args)
    end

    def delete_message(**args)
      @logger.debug "delete_message(#{args.inspect})"
      @sqs.delete_message(**args)
    end

    def delete_message_batch(**args)
      @logger.debug "delete_message_batch(#{args.inspect})"
      @sqs.delete_message_batch(**args)
    end

  end   # class SQSClientWrapper


  class SQSMessage

    SQS_EVENT_SOURCE = 'bricolage:system'

    # Writer interface
    def SQSMessage.create(
        name:,
        time: Time.now.getutc,
        source: SQS_EVENT_SOURCE,
        delay_seconds: 0,
        **message_params)
      new(name: name, time: time, source: source, delay_seconds: delay_seconds, **message_params)
    end

    def SQSMessage.for_sqs_result(result)
      result.messages.flat_map {|msg|
        body = JSON.parse(msg.body)
        records = body['Records'] or next [UnknownSQSMessage.for_sqs_record(msg, nil)]
        records.map {|rec| get_concrete_class(msg, rec).for_sqs_record(msg, rec) }
      }
    end

    # abstract SQSMessage.get_concrete_class(msg, rec)

    def SQSMessage.for_sqs_record(msg, rec)
      new(** SQSMessage.parse_sqs_record(msg, rec).merge(parse_sqs_record(msg, rec)))
    end

    def SQSMessage.parse_sqs_record(msg, rec)
      {
        message_id: msg.message_id,
        receipt_handle: msg.receipt_handle,
        name: (rec ? rec['eventName'] : nil),
        time: get_event_time(rec),
        source: (rec ? rec['eventSource'] : nil)
      }
    end

    def SQSMessage.get_event_time(rec)
      return nil unless rec
      str = rec['eventTime'] or return nil
      Time.parse(str) rescue nil
    end

    def initialize(name:, time:, source:,
        message_id: nil, receipt_handle: nil, delay_seconds: nil,
        **message_params)
      @name = name
      @time = time
      @source = source

      @message_id = message_id
      @receipt_handle = receipt_handle

      @delay_seconds = delay_seconds

      init_message(**message_params)
    end

    # abstract init_message(**message_params)

    attr_reader :name
    attr_reader :time
    attr_reader :source

    # Valid only for received messages

    attr_reader :message_id
    attr_reader :receipt_handle

    # Valid only for sending messages

    attr_reader :delay_seconds

    def body
      obj = {}
      [
        ['eventName', @name],
        ['eventTime', (@time ? @time.iso8601 : nil)],
        ['eventSource', @source]
      ].each do |name, value|
        obj[name] = value if value
      end
      obj
    end

  end   # class SQSMessage


  class UnknownSQSMessage < SQSMessage

    def UnknownSQSMessage.parse_sqs_record(msg, _rec)
      { message_body: msg.body }
    end

    def message_type
      'unknown'
    end

    def init_message(message_body:)
      @message_body = message_body
    end

    attr_reader :message_body

  end   # class UnknownSQSMessage


  # library private
  class ObjectMessage

    def initialize(body, delay_seconds: 0)
      @body = body
      @delay_seconds = delay_seconds
    end

    attr_reader :body
    attr_reader :delay_seconds

  end

end   # module Bricolage
