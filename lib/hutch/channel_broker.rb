require 'hutch/logging'
require 'hutch/broker_handlers'

module Hutch
  class ChannelBroker
    include Logging
    include BrokerHandlers

    attr_accessor :connection

    def initialize(connection, config)
      @connection = connection
      @config = config || Hutch::Config
    end

    def disconnect
      @channel.close if active
      @channel = nil
      @exchange = nil
      @default_wait_exchange = nil
      @wait_exchanges = nil
    end

    def reconnect
      disconnect
      open_channel!
    end

    def channel
      return @channel if active
      reconnect
    end

    def exchange
      return @exchange if active
      reconnect
      @exchange
    end

    def wait_exchanges
      return {} if @config[:mq_wait_exchange].nil?
      reconnect unless active
      @wait_exchanges ||= set_up_wait_exchanges
    end

    def default_wait_exchange
      return nil if @config[:mq_wait_exchange].nil?
      reconnect unless active
      @default_wait_exchange ||= set_up_default_wait_exchange
    end

    def active
      @channel && @channel.active
    end

    def open_channel
      logger.info "opening rabbitmq channel with pool size #{consumer_pool_size}, abort on exception #{consumer_pool_abort_on_exception}"
      connection.create_channel(nil, consumer_pool_size, consumer_pool_abort_on_exception).tap do |ch|
        connection.prefetch_channel(ch, @config[:channel_prefetch])
        if @config[:publisher_confirms] || @config[:force_publisher_confirms]
          logger.info 'enabling publisher confirms'
          ch.confirm_select
        end
      end
    end

    def open_channel!
      @channel = open_channel
    end

    def declare_exchange(ch = channel)
      exchange_name = @config[:mq_exchange]
      exchange_options = { durable: true }.merge(@config[:mq_exchange_options])
      logger.info "using topic exchange '#{exchange_name}'"

      with_bunny_precondition_handler('exchange') do
        ch.topic(exchange_name, exchange_options)
      end
    end

    def declare_exchange!(*args)
      @exchange = declare_exchange(*args)
    end

    private

    def set_up_wait_exchanges
      wait_exchange_name = @config[:mq_wait_exchange]
      wait_queue_name = @config[:mq_wait_queue]

      expiration_suffices = (@config[:mq_wait_expiration_suffices] || []).map(&:to_s)

      @wait_exchanges = expiration_suffices.each_with_object({}) do |suffix, hash|
        logger.info "using expiration suffix '_#{suffix}'"

        suffix_exchange = declare_wait_exchange("#{wait_exchange_name}_#{suffix}")
        hash[suffix] = suffix_exchange
        declare_wait_queue(suffix_exchange, "#{wait_queue_name}_#{suffix}")
      end
    end

    def set_up_default_wait_exchange
      wait_exchange_name = @config[:mq_wait_exchange]
      wait_queue_name = @config[:mq_wait_queue]

      logger.info "using fanout wait exchange '#{wait_exchange_name}'"

      @default_wait_exchange = declare_wait_exchange(wait_exchange_name)

      logger.info "using wait queue '#{wait_queue_name}'"

      declare_wait_queue(@default_wait_exchange, wait_queue_name)
      @default_wait_exchange
    end

    def declare_wait_exchange(name)
      with_bunny_precondition_handler('exchange') do
        channel.fanout(name, durable: true)
      end
    end

    def declare_wait_queue(exchange, queue_name)
      with_bunny_precondition_handler('queue') do
        queue = channel.queue(
          queue_name,
          durable: true,
          arguments: { 'x-dead-letter-exchange' => @config[:mq_exchange] }
        )
        queue.bind(exchange)
      end
    end

    def consumer_pool_size
      @config[:consumer_pool_size]
    end

    def consumer_pool_abort_on_exception
      @config[:consumer_pool_abort_on_exception]
    end
  end
end
