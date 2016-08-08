require 'bricolage/datasource'
require 'bricolage/sqswrapper'
require 'securerandom'
require 'aws-sdk'
require 'json'
require 'time'

module Bricolage

  class SQSDataSource < DataSource

    declare_type 'sqs'

    def initialize(region: 'ap-northeast-1', url:, access_key_id:, secret_access_key:,
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

    def client
      @client ||= begin
        c = @noop ? DummySQSClient.new : Aws::SQS::Client.new(region: @region, access_key_id: @access_key_id, secret_access_key: @secret_access_key)
        SQSClientWrapper.new(c, logger: logger)
      end
    end

    #
    # High-Level Polling Interface
    #

    def main_handler_loop(handlers:, message_class:)
      trap_signals

      n_zero = 0
      until terminating?
        insert_handler_wait(n_zero)
        n_msg = handle_messages(handlers: handlers, message_class: message_class)
        if n_msg == 0
          n_zero += 1
        else
          n_zero = 0
        end
        delete_message_buffer.flush
      end
      delete_message_buffer.flush_force
      logger.info "shutdown gracefully"
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

    def initiate_terminate
      # No I/O allowed in this method
      @terminating = true
    end

    def terminating?
      @terminating
    end

    def insert_handler_wait(n_zero)
      sec = 2 ** [n_zero, 6].min   # max 64s
      logger.info "queue wait: sleep #{sec}" if n_zero > 0
      sleep sec
    end

    def handle_messages(handlers:, message_class:)
      n_msg = foreach_message(message_class) do |msg|
        logger.debug "handling message: #{msg.inspect}" if logger.debug?
        mid = "handle_#{msg.message_type}"
        # just ignore unknown event to make app migration easy
        if handlers.respond_to?(mid, true)
          handlers.__send__(mid, msg)
        else
          logger.error "unknown SQS message type: #{msg.message_type.inspect} (message-id: #{msg.message_id})"
        end
      end
      n_msg
    end

    def foreach_message(message_class, &block)
      result = receive_messages()
      unless result and result.successful?
        logger.error "ReceiveMessage failed: #{result ? result.error.message : '(result=nil)'}"
        return nil
      end
      logger.info "receive #{result.messages.size} messages" unless result.messages.empty?
      msgs = message_class.for_sqs_result(result)
      msgs.each(&block)
      msgs.size
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

    def delete_message(msg)
      client.delete_message(
        queue_url: @url,
        receipt_handle: msg.receipt_handle
      )
    end

    def delete_message_async(msg)
      delete_message_buffer.put(msg)
    end

    def delete_message_buffer
      @delete_message_buffer ||= DeleteMessageBuffer.new(client, @url, logger)
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
        entries = @buf.values.select {|ent| ent.issurable?(now) }
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

        def issurable?(now)
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
        records = body['Records'] or next []
        records.map {|rec| get_concrete_class(msg, rec).for_sqs_record(msg, rec) }
      }
    end

    # abstract SQSMessage.get_concrete_class(msg, rec)

    def SQSMessage.for_sqs_record(msg, rec)
      new(** SQSMessage.parse_sqs_record(msg, rec).merge(parse_sqs_record(msg, rec)))
    end

    def SQSMessage.parse_sqs_record(msg, rec)
      time_str = rec['eventTime']
      tm = time_str ? (Time.parse(time_str) rescue nil) : nil
      {
        message_id: msg.message_id,
        receipt_handle: msg.receipt_handle,
        name: rec['eventName'],
        time: tm,
        source: rec['eventSource']
      }
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

end   # module Bricolage
