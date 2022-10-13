require 'securerandom'
require 'forwardable'
require 'hutch/logging'
require 'hutch/exceptions'

module Hutch
  class Publisher
    include Logging
    extend Forwardable

    attr_reader :connection, :broker, :config
    def_delegators :broker,
                   :channel,
                   :exchange,
                   :default_wait_exchange,
                   :wait_exchanges

    def initialize(connection, broker, config = Hutch::Config)
      @connection = connection
      @broker     = broker
      @config     = config
    end

    def publish(routing_key, message, properties = {}, options = {})
      ensure_connection!(routing_key, message)

      serializer = options[:serializer] || config[:serializer]

      non_overridable_properties = {
        routing_key:  routing_key,
        timestamp:    connection.current_timestamp,
        content_type: serializer.content_type
      }
      properties[:message_id] ||= generate_id

      payload = serializer.encode(message)

      log_publication(serializer, payload, routing_key, exchange)

      response = exchange.publish(payload, { persistent: true }
        .merge(properties)
        .merge(global_properties)
        .merge(non_overridable_properties))

      wait_for_confirms_or_raise(routing_key, message) if config[:force_publisher_confirms]
      response
    end

    def publish_wait(routing_key, message, properties = {}, options = {})
      ensure_connection!(routing_key, message)
      if config[:mq_wait_exchange].nil?
        raise_publish_error('wait exchange not defined', routing_key, message)
      end

      serializer = options[:serializer] || config[:serializer]

      non_overridable_properties = {
        routing_key: routing_key,
        timestamp: connection.current_timestamp,
        content_type: serializer.content_type
      }
      properties[:message_id] ||= generate_id

      payload = serializer.encode(message)

      message_properties = { persistent: true }
                           .merge(properties)
                           .merge(global_properties)
                           .merge(non_overridable_properties)
      expiration = message_properties[:expiration]
      exchange = default_wait_exchange
      if expiration
        exchange = wait_exchanges[expiration.to_s]
        exchange ||= broker.declare_wait_exchange(expiration.to_s)
      end

      log_publication(serializer, payload, routing_key, exchange)

      response = exchange.publish(payload, message_properties)

      wait_for_confirms_or_raise(routing_key, message) if config[:force_publisher_confirms]
      response
    end

    private

    def log_publication(serializer, payload, routing_key, exchange)
      logger.debug do
        spec =
          if serializer.binary?
            "#{payload.bytesize} bytes message"
          else
            "message '#{payload}'"
          end
        "publishing #{spec} to '#{exchange.name}' with routing key '#{routing_key}'"
      end
    end

    def wait_for_confirms_or_raise(routing_key, message)
      unless channel.wait_for_confirms
        raise_publish_error('Message not acknowledged by broker', routing_key, message)
      end
    end

    def raise_publish_error(reason, routing_key, message)
      msg = "unable to publish - #{reason}. Message: #{JSON.dump(message)}, Routing key: #{routing_key}."
      logger.error(msg)
      raise PublishError, msg
    end

    def ensure_connection!(routing_key, message)
      raise_publish_error('no connection to broker', routing_key, message) unless connection
      raise_publish_error('connection is closed', routing_key, message) unless connection.open?
    end

    def generate_id
      SecureRandom.uuid
    end

    def global_properties
      Hutch.global_properties.respond_to?(:call) ? Hutch.global_properties.call : Hutch.global_properties
    end
  end
end
