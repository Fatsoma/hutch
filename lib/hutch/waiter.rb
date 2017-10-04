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
      self.sig_read, self.sig_write = IO.pipe
      register_signal_handlers

      self.action_read, self.action_write = IO.pipe
      Thread.main[:action_queue] = Queue.new
    end

    def wait_until_signaled
      loop do
        read_pipes = wait_for_signal.first
        break unless read_pipes.all? { |pipe| read_pipe(pipe) }
      end
    end

    def push_action(action, delivery_info, properties, ex)
      Thread.main[:action_queue] << [action, delivery_info, properties, ex]
      action_write.write("#{delivery_info.delivery_tag}\n")
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
      false
    end

    # return true to continue processing
    def handle_action(_delivery_tag)
      action, delivery_info, properties, ex = Thread.main[:action_queue].pop
      # TODO: check delivery_tag ??
      case action
      when :ack then broker.ack(delivery_info.delivery_tag)
      when :nack then acknowledge_error(delivery_info, properties, ex)
      else raise "Assertion failed - unhandled action: #{action.inspect}"
      end
      true
    end

    def acknowledge_error(delivery_info, properties, ex)
      acks = error_acknowledgements +
        [Hutch::Acknowledgements::NackOnAllFailures.new]
      acks.find do |backend|
        backend.handle(delivery_info, properties, broker, ex)
      end
    end

    private

    def read_pipe(pipe)
      case pipe
      when sig_read
        sig = sig_read.gets.chomp
        handle_signal(sig)
      when action_read
        delivery_tag = action_read.gets.chomp
        handle_action(delivery_tag)
      end
    end

    def log_thread_backtraces
      logger.info 'Requested a VM-wide thread stack trace dump...'
      Thread.list.each do |thread|
        logger.info "Thread TID-#{thread.object_id.to_s(36)} #{thread['label']}"
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
    attr_accessor :sig_read, :sig_write, :action_read, :action_write

    def wait_for_signal
      IO.select([sig_read, action_read])
    end

    def register_signal_handlers
      REGISTERED_SIGNALS.each do |sig|
        # This needs to be reentrant, so we queue up signals to be handled
        # in the run loop, rather than acting on signals here
        trap(sig) do
          sig_write.puts(sig)
        end
      end
    end

    def user_signal?(sig)
      USER_SIGNALS.include?(sig)
    end

    def error_acknowledgements
      Hutch::Config[:error_acknowledgements]
    end
  end
end
