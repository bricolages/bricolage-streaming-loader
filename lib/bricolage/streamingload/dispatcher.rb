require 'bricolage/context'
require 'bricolage/exception'
require 'bricolage/version'
require 'bricolage/sqsdatasource'
require 'bricolage/logger'
require 'bricolage/streamingload/event'
require 'bricolage/streamingload/objectbuffer'
require 'bricolage/streamingload/urlpatterns'
require 'bricolage/streamingload/alertinglogger'
require 'aws-sdk'
require 'yaml'
require 'optparse'
require 'fileutils'

module Bricolage

  module StreamingLoad

    class Dispatcher < SQSDataSource::MessageHandler

      def Dispatcher.main
        opts = DispatcherOptions.new(ARGV)
        opts.parse
        unless opts.rest_arguments.size == 1
          $stderr.puts opts.usage
          exit 1
        end
        config_path, * = opts.rest_arguments
        config = YAML.load(File.read(config_path))
        log = opts.log_file_path ? new_logger(File.expand_path(opts.log_file_path), config) : nil
        ctx = Context.for_application('.', environment: opts.environment, logger: log)
        logger = raw_logger = ctx.logger
        event_queue = ctx.get_data_source('sqs', config.fetch('event-queue-ds', 'sqs_event'))
        task_queue = ctx.get_data_source('sqs', config.fetch('task-queue-ds', 'sqs_task'))
        if config['alert-level']
          logger = AlertingLogger.new(
            logger: raw_logger,
            sns_datasource: ctx.get_data_source('sns', config.fetch('sns-ds', 'sns')),
            alert_level: config.fetch('alert-level', 'warn')
          )
        end

        object_buffer = ObjectBuffer.new(
          control_data_source: ctx.get_data_source('sql', config.fetch('ctl-postgres-ds', 'db_ctl')),
          logger: logger
        )

        url_patterns = URLPatterns.for_config(config.fetch('url_patterns'))

        dispatcher = Dispatcher.new(
          event_queue: event_queue,
          task_queue: task_queue,
          object_buffer: object_buffer,
          url_patterns: url_patterns,
          dispatch_interval: config.fetch('dispatch-interval', 60),
          logger: logger
        )

        Process.daemon(true) if opts.daemon?
        create_pid_file opts.pid_file_path if opts.pid_file_path
        Dir.chdir '/'
        dispatcher.event_loop
      rescue Exception => e
        logger.exception e
        logger.error "dispatcher abort: pid=#{$$}"
        raise
      end

      def Dispatcher.new_logger(path, config)
        Logger.new(
          device: path,
          rotation_period: config.fetch('log-rotation-period', 'daily'),
          rotation_size: config.fetch('log-rotation-size', nil)
        )
      end

      def Dispatcher.create_pid_file(path)
        File.open(path, 'w') {|f|
          f.puts $$
        }
      rescue
        # ignore
      end

      def initialize(event_queue:, task_queue:, object_buffer:, url_patterns:, dispatch_interval:, logger:)
        @event_queue = event_queue
        @task_queue = task_queue
        @object_buffer = object_buffer
        @url_patterns = url_patterns
        @dispatch_interval = dispatch_interval
        @dispatch_message_id = nil
        @logger = logger
        @dispatch_requested = false
        @checkpoint_requested = false
      end

      attr_reader :logger

      def event_loop
        logger.info "*** dispatcher started: pid=#{$$}"
        set_dispatch_timer
        @event_queue.handle_messages(handler: self, message_class: Event)
        @event_queue.process_async_delete_force
        logger.info "*** shutdown gracefully: pid=#{$$}"
      end

      # override
      def after_message_batch
        # must be processed first
        @event_queue.process_async_delete

        if @dispatch_requested
          logger.info "*** dispatch requested"
          dispatch_tasks
          @dispatch_requested = false
        end

        if @checkpoint_requested
          create_checkpoint
          @checkpoint_requested = false   # is needless, but reset it just in case
        end
      end

      def handle_unknown(e)
        logger.warn "unknown event: #{e.message_body}"
        @event_queue.delete_message_async(e)
      end

      def handle_shutdown(e)
        logger.info "*** shutdown requested"
        @event_queue.initiate_terminate
        # Delete this event immediately
        @event_queue.delete_message(e)
      end

      def handle_checkpoint(e)
        # Delay creating CHECKPOINT after the current message batch,
        # because any other extra events are already received.
        @checkpoint_requested = true
        # Delete this event immediately
        @event_queue.delete_message(e)
      end

      def create_checkpoint
        logger.info "*** checkpoint requested"
        logger.info "Force-flushing all objects..."
        tasks = @object_buffer.flush_tasks_force
        send_tasks tasks
        logger.info "All objects flushed; shutting down..."
        @event_queue.initiate_terminate
      end

      def handle_data(e)
        unless e.created?
          @event_queue.delete_message_async(e)
          return
        end
        obj = e.loadable_object(@url_patterns)
        @object_buffer.put(obj)
        @event_queue.delete_message_async(e)
      end

      def handle_dispatch(e)
        # Dispatching tasks may takes 10 minutes or more, it can exceeds visibility timeout.
        # To avoid this, delay dispatching until all events of current message batch are processed.
        if @dispatch_message_id == e.message_id
          @dispatch_requested = true
        end
        @event_queue.delete_message_async(e)
      end

      def dispatch_tasks
        tasks = @object_buffer.flush_tasks
        send_tasks tasks
        set_dispatch_timer
      end

      def set_dispatch_timer
        res = @event_queue.send_message(DispatchEvent.create(delay_seconds: @dispatch_interval))
        @dispatch_message_id = res.message_id
      end

      def handle_flushtable(e)
        logger.info "*** flushtable requested: table=#{e.table_name}"
        tasks = @object_buffer.flush_table_force(e.table_name)
        send_tasks tasks
        # Delete this event immediately
        @event_queue.delete_message(e)
      end

      def send_tasks(tasks)
        tasks.each do |task|
          @task_queue.put task
        end
      end

    end


    class DispatcherOptions

      def initialize(argv)
        @argv = argv
        @daemon = false
        @log_file_path = nil
        @pid_file_path = nil
        @rest_arguments = nil

        @opts = opts = OptionParser.new("Usage: #{$0} CONFIG_PATH")
        opts.on('--task-id=id', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_id|
          @task_id = task_id
        }
        opts.on('-e', '--environment=NAME', "Sets execution environment [default: #{Context::DEFAULT_ENV}]") {|env|
          @environment = env
        }
        opts.on('--daemon', 'Becomes daemon in server mode.') {
          @daemon = true
        }
        opts.on('--log-file=PATH', 'Log file path') {|path|
          @log_file_path = path
        }
        opts.on('--pid-file=PATH', 'Creates PID file.') {|path|
          @pid_file_path = path
        }
        opts.on('--help', 'Prints this message and quit.') {
          puts opts.help
          exit 0
        }
        opts.on('--version', 'Prints version and quit.') {
          puts "#{File.basename($0)} version #{VERSION}"
          exit 0
        }
      end

      def usage
        @opts.help
      end

      def parse
        @opts.parse!(@argv)
        @rest_arguments = @argv.dup
      rescue OptionParser::ParseError => err
        raise OptionError, err.message
      end

      attr_reader :rest_arguments, :environment, :log_file_path

      def daemon?
        @daemon
      end

      attr_reader :pid_file_path

    end

  end   # module StreamingLoad

end   # module Bricolage
