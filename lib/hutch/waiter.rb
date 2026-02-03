require 'hutch/logging'
require 'hutch/acknowledgements/nack_on_all_failures'
require 'thread'

module Hutch
  # Signal-handling class.
  #
  # Currently, the signal USR2 performs a thread dump,
  # while QUIT, TERM and INT all perform a graceful shutdown.
  class Waiter
    include Logging

    def self.supported_signals_of(list)
      list.keep_if { |s| Signal.list.keys.include?(s) }.tap do |result|
        result.delete('QUIT') if defined?(JRUBY_VERSION)
      end
    end

    SHUTDOWN_SIGNALS = supported_signals_of(%w(QUIT TERM INT)).freeze
    # We have chosen a JRuby-supported signal
    USER_SIGNALS = supported_signals_of(%w(USR2)).freeze
    REGISTERED_SIGNALS = (SHUTDOWN_SIGNALS + USER_SIGNALS).freeze

    def initialize(broker)
      @broker = broker
    end

    def register_handlers
      Thread.main[:action_queue] = Queue.new
      register_signal_handlers
    end

    def wait_until_signaled
      # Block and wait for messages
      while (event = action_queue.pop)
        type, data = event

        case type
        when :signal
          # Return false breaks the loop for graceful shutdown
          break unless handle_signal(data)
        when :action
          handle_action(data)
        else
          raise "Assertion failed - unhandled event: #{type}"
        end
      end
    end

    # Consumer threads call this to push work to the main thread
    def push_action(action, delivery_info, properties, ex)
      action_queue << [:action, {
        action: action,
        delivery_info: delivery_info,
        properties: properties,
        ex: ex,
        pushed_at: Time.now.to_f
      }]
    end

    # return true to continue processing
    def handle_signal(sig)
      return true unless REGISTERED_SIGNALS.include?(sig)
      if user_signal?(sig)
        handle_user_signal(sig)
      else
        handle_shutdown_signal(sig)
      end
    end

    def handle_user_signal(sig)
      case sig
      when 'USR2' then log_thread_backtraces
      else raise "Assertion failed - unhandled signal: #{sig.inspect}"
      end
      true
    end

    def handle_shutdown_signal(sig)
      logger.info "caught SIG#{sig}, stopping hutch..."
      drain_actions
      false
    end

    def handle_action(data)
      latency_ms = (Time.now.to_f - data[:pushed_at]) * 1000

      if latency_ms > 5000
        logger.warn "Queue latency exceeded 5000ms (actual #{latency_ms}ms)"
      end

      case data[:action]
      when :ack then broker.ack(data[:delivery_info].delivery_tag)
      when :nack then acknowledge_error(data[:delivery_info], data[:properties], data[:ex])
      else raise "Assertion failed - unhandled action: #{action}"
      end
    rescue => e
      logger.error "Error during #{data[:action]}: #{e.message}"
      raise e
    end

    def acknowledge_error(delivery_info, properties, ex)
      acks = error_acknowledgements +
             [Hutch::Acknowledgements::NackOnAllFailures.new]
      acks.find do |backend|
        backend.handle(delivery_info, properties, broker, ex)
      end
    end

    private

    def log_thread_backtraces
      logger.info 'Requested a VM-wide thread stack trace dump...'
      Thread.list.each do |thread|
        main_label = thread == Thread.main ? 'main' : ''
        logger.info "Thread TID-#{thread.object_id.to_s(36)} #{thread['label']} #{main_label}"
        logger.info backtrace_for(thread)
      end
    end

    def backtrace_for(thread)
      if thread.backtrace
        thread.backtrace.join("\n")
      else
        '<no backtrace available>'
      end
    end

    attr_reader :broker

    def register_signal_handlers
      REGISTERED_SIGNALS.each do |sig|
        trap(sig) do
          action_queue << [:signal, sig]
        end
      end
    end

    def user_signal?(sig)
      USER_SIGNALS.include?(sig)
    end

    def error_acknowledgements
      Hutch::Config[:error_acknowledgements] || []
    end

    def action_queue
      queue = Thread.main[:action_queue]
      raise 'Undefined main thread queue' unless queue
      queue
    end

    # Drain the queue during shutdown
    def drain_actions
      queue = action_queue

      until queue.empty?
        begin
          type, data = queue.pop(true)
          handle_action(data) if type == :action
        rescue ThreadError
          break
        end
      end
    end
  end
end
