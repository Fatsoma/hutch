require 'hutch/message'
require 'hutch/logging'
require 'hutch/broker'
require 'hutch/acknowledgements/nack_on_all_failures'
require 'carrot-top'
require 'thread'

module Hutch
  class Worker
    include Logging

    SHUTDOWN_SIGNALS = %w(QUIT TERM INT)

    def initialize(broker, consumers)
      @broker        = broker
      self.consumers = consumers
    end

    # Run the main event loop. The consumers will be set up with queues, and
    # process the messages in their respective queues indefinitely. This method
    # never returns.
    def run
      # Set up signal handlers for graceful shutdown
      register_signal_handlers

      setup_queues

      main_loop
    end

    def main_loop
      if defined?(JRUBY_VERSION)
        # Binds shutdown listener to notify main thread if channel was closed
        bind_shutdown_handler

        handle_signals until shutdown_not_called?(0.1)
      else
        # Take a break from Thread#join every 0.1 seconds to check if we've
        # been sent any signals
        handle_signals until @broker.wait_on_threads(0.1)
      end
    end

    # Register handlers for SIG{QUIT,TERM,INT} to shut down the worker
    # gracefully. Forceful shutdowns are very bad!
    def register_signal_handlers
      Thread.main[:signal_queue] = []
      supported_shutdown_signals.each do |sig|
        # This needs to be reentrant, so we queue up signals to be handled
        # in the run loop, rather than acting on signals here
        trap(sig) do
          Thread.main[:signal_queue] << sig
        end
      end
    end

    # Handle any pending signals
    def handle_signals
      signal = Thread.main[:signal_queue].shift
      if signal
        logger.info "caught sig#{signal.downcase}, stopping hutch..."
        stop
      end
    end

    # Stop a running worker by killing all subscriber threads.
    def stop
      @broker.stop
    end

    # Binds shutdown handler, called if channel is closed or network Failed
    def bind_shutdown_handler
      @broker.channel.on_shutdown do
        Thread.main[:shutdown_received] = true
      end
    end

    # Checks if shutdown handler was called, then sleeps for interval
    def shutdown_not_called?(interval)
      if Thread.main[:shutdown_received]
        true
      else
        sleep(interval)
        false
      end
    end

    # Set up the queues for each of the worker's consumers.
    def setup_queues
      logger.info 'setting up queues'
      @consumers.each { |consumer| setup_queue(consumer) }
    end

    # Bind a consumer's routing keys to its queue, and set up a subscription to
    # receive messages sent to the queue.
    def setup_queue(consumer)
      queue = @broker.queue(consumer.get_queue_name, consumer.get_arguments)
      @broker.bind_queue(queue, consumer.routing_keys)

      queue.subscribe(manual_ack: true) do |*args|
        delivery_info, properties, payload = Hutch::Adapter.decode_message(*args)
        handle_message(consumer, delivery_info, properties, payload)
      end
    end

    # Called internally when a new messages comes in from RabbitMQ. Responsible
    # for wrapping up the message and passing it to the consumer.
    def handle_message(consumer, delivery_info, properties, payload)
      serializer = consumer.get_serializer || Hutch::Config[:serializer]
      logger.info do
        spec = serializer.binary? ? "#{payload.bytesize} bytes" : "#{payload}"
        "message(#{properties.message_id || '-'}): " \
        "routing key: #{delivery_info.routing_key}, " \
        "consumer: #{consumer}, " \
        "payload: #{spec}"
      end

      message = Message.new(delivery_info, properties, payload, serializer)
      consumer_instance = consumer.new
      consumer_instance.broker = @broker
      consumer_instance.delivery_info = delivery_info
      with_tracing(consumer_instance).handle(message)
      @broker.ack(delivery_info.delivery_tag)
    rescue => ex
      acknowledge_error(delivery_info, properties, @broker, ex)
      handle_error(properties.message_id, payload, consumer, ex)
    end

    def with_tracing(klass)
      Hutch::Config[:tracer].new(klass)
    end

    def handle_error(message_id, payload, consumer, ex)
      Hutch::Config[:error_handlers].each do |backend|
        backend.handle(message_id, payload, consumer, ex)
      end
    end

    def acknowledge_error(delivery_info, properties, broker, ex)
      acks = error_acknowledgements +
        [Hutch::Acknowledgements::NackOnAllFailures.new]
      acks.find do |backend|
        backend.handle(delivery_info, properties, broker, ex)
      end
    end

    def consumers=(val)
      if val.empty?
        logger.warn "no consumer loaded, ensure there's no configuration issue"
      end
      @consumers = val
    end

    def error_acknowledgements
      Hutch::Config[:error_acknowledgements]
    end

    private

    def supported_shutdown_signals
      SHUTDOWN_SIGNALS.keep_if { |s| Signal.list.keys.include? s }.map(&:to_sym)
    end
  end
end
