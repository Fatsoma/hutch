require 'hutch/message'
require 'hutch/logging'
require 'hutch/broker'
require 'hutch/waiter'
require 'carrot-top'
require 'securerandom'

module Hutch
  class Worker
    include Logging

    def initialize(broker, consumers, setup_procs)
      @broker        = broker
      self.waiter    = Waiter.new(broker)
      self.consumers = consumers
      self.setup_procs = setup_procs
    end

    # Run the main event loop. The consumers will be set up with queues, and
    # process the messages in their respective queues indefinitely. This method
    # never returns.
    def run
      waiter.register_handlers

      setup_queues
      setup_procs.each(&:call)

      waiter.wait_until_signaled

      stop
    end

    # Stop a running worker by killing all subscriber threads.
    def stop
      @broker.stop
    end

    # Set up the queues for each of the worker's consumers.
    def setup_queues
      logger.info 'setting up queues'
      vetted = @consumers.reject { |c| group_configured? && group_restricted?(c) }
      vetted.each do |c|
        setup_queue(c)
      end
    end

    # Bind a consumer's routing keys to its queue, and set up a subscription to
    # receive messages sent to the queue.
    def setup_queue(consumer)
      logger.info "setting up queue: #{consumer.get_queue_name}"

      queue = @broker.queue(consumer.get_queue_name, consumer.get_options)
      @broker.bind_queue(queue, consumer.routing_keys)

      queue.subscribe(consumer_tag: unique_consumer_tag, manual_ack: true) do |*args|
        delivery_info, properties, payload = Hutch::Adapter.decode_message(*args)
        handle_message(consumer, delivery_info, properties, payload)
      end
    end

    # Called internally when a new messages comes in from RabbitMQ. Responsible
    # for wrapping up the message and passing it to the consumer.
    # rubocop:disable Metrics/CyclomaticComplexity
    def handle_message(consumer, delivery_info, properties, payload)
      serializer = consumer.get_serializer || Hutch::Config[:serializer]
      logger.debug do
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
      @broker.ack(delivery_info.delivery_tag) unless consumer_instance.message_rejected?
    rescue => ex
      acknowledge_error(delivery_info, properties, @broker, ex)
      handle_error(properties, payload, consumer, ex, delivery_info)
    end
    # rubocop:enable Metrics/CyclomaticComplexity

    def with_tracing(klass)
      Hutch::Config[:tracer].new(klass)
    end

    def handle_error(*args)
      Hutch::Config[:error_handlers].each do |backend|
        backend.handle *args.first(backend.method(:handle).arity)
      end
    end

    def consumers=(val)
      if val.empty?
        logger.warn "no consumer loaded, ensure there's no configuration issue"
      end
      @consumers = val
    end

    private

    def group_configured?
      if group.present? && consumer_groups.blank?
        logger.info 'Consumer groups are blank'
      end
      group.present?
    end

    def group_restricted?(consumer)
      consumers_to_load = consumer_groups[group]
      if consumers_to_load
        !consumers_to_load.include?(consumer.name)
      else
        true
      end
    end

    def group
      Hutch::Config[:group]
    end

    def consumer_groups
      Hutch::Config[:consumer_groups]
    end

    attr_accessor :setup_procs, :waiter

    def unique_consumer_tag
      prefix = Hutch::Config[:consumer_tag_prefix]
      unique_part = SecureRandom.uuid
      "#{prefix}-#{unique_part}".tap do |tag|
        raise "Tag must be 255 bytes long at most, current one is #{tag.bytesize} ('#{tag}')" if tag.bytesize > 255
      end
    end
  end
end
