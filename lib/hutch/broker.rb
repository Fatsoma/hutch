require 'active_support/core_ext/object/blank'

require 'carrot-top'
require 'forwardable'
require 'hutch/logging'
require 'hutch/exceptions'
require 'hutch/publisher'
require 'hutch/broker_handlers'
require 'hutch/channel_broker'

module Hutch
  class Broker
    include Logging
    extend Forwardable
    include BrokerHandlers

    attr_accessor :connection, :api_client
    def_delegators :channel_broker,
                   :channel,
                   :exchange,
                   :default_wait_exchange,
                   :wait_exchanges

    CHANNEL_BROKER_KEY = :hutch_channel_broker


    DEFAULT_AMQP_PORT =
      case RUBY_ENGINE
      when "jruby" then
        com.rabbitmq.client.ConnectionFactory::DEFAULT_AMQP_PORT
      when "ruby" then
        AMQ::Protocol::DEFAULT_PORT
      end

    DEFAULT_AMQPS_PORT =
      case RUBY_ENGINE
      when "jruby" then
        com.rabbitmq.client.ConnectionFactory::DEFAULT_AMQP_OVER_SSL_PORT
      when "ruby" then
        AMQ::Protocol::TLS_PORT
      end


    # @param config [nil,Hash] Configuration override
    def initialize(config = nil)
      @config = config || Hutch::Config
    end

    # Connect to broker
    #
    # @example
    #   Hutch::Broker.new.connect(enable_http_api_use: true) do
    #     # will disconnect after this block
    #   end
    #
    # @param [Hash] options The options to connect with
    # @option options [Boolean] :enable_http_api_use
    def connect(options = {})
      @options = options
      set_up_amqp_connection
      if http_api_use_enabled?
        logger.info 'HTTP API use is enabled'
        set_up_api_connection
      else
        logger.info 'HTTP API use is disabled'
      end

      if tracing_enabled?
        logger.info "tracing is enabled using #{@config[:tracer]}"
      else
        logger.info 'tracing is disabled'
      end

      return unless block_given?
      begin
        yield
      ensure
        disconnect
      end
    end

    def disconnect
      channel_broker.disconnect unless Thread.current[CHANNEL_BROKER_KEY].nil?
      Thread.current[CHANNEL_BROKER_KEY] = nil
      @connection.close if @connection
      @connection = nil
      @api_client = nil
      @publisher = nil
    end

    # Connect to RabbitMQ via AMQP
    #
    # This sets up the main connection and channel we use for talking to
    # RabbitMQ. It also ensures the existence of the exchange we'll be using.
    def set_up_amqp_connection
      open_connection!
      open_channel!
      declare_exchange!
      declare_publisher!
    end

    def open_connection
      logger.info "connecting to rabbitmq (#{sanitized_uri})"

      connection = Hutch::Adapter.new(connection_params)

      with_bunny_connection_handler(sanitized_uri) do
        connection.start
      end

      logger.info "connected to RabbitMQ at #{connection_params[:host]} as #{connection_params[:username]}"
      connection
    end

    def open_connection!
      @connection = open_connection
    end

    def open_channel
      channel_broker.open_channel
    end

    def open_channel!
      channel_broker.open_channel!
    end

    def declare_exchange(ch = channel)
      channel_broker.declare_exchange(ch)
    end

    def declare_exchange!(*args)
      channel_broker.declare_exchange!(*args)
    end

    def declare_publisher!
      @publisher = Hutch::Publisher.new(connection, self, @config)
    end

    # Set up the connection to the RabbitMQ management API. Unfortunately, this
    # is necessary to do a few things that are impossible over AMQP. E.g.
    # listing queues and bindings.
    def set_up_api_connection
      logger.info "connecting to rabbitmq HTTP API (#{api_config.sanitized_uri})"

      with_authentication_error_handler do
        with_connection_error_handler do
          @api_client = CarrotTop.new(host: api_config.host, port: api_config.port,
                                      user: api_config.username, password: api_config.password,
                                      ssl: api_config.ssl)
          @api_client.exchanges
        end
      end
    end

    def http_api_use_enabled?
      op = @options.fetch(:enable_http_api_use, true)
      cf = if @config[:enable_http_api_use].nil?
             true
           else
             @config[:enable_http_api_use]
           end

      op && cf
    end

    def tracing_enabled?
      @config[:tracer] && @config[:tracer] != Hutch::Tracers::NullTracer
    end

    # Create / get a durable queue and apply namespace if it exists.
    def queue(name, arguments = {})
      with_bunny_precondition_handler('queue') do
        namespace = @config[:namespace].to_s.downcase.gsub(/[^-:\.\w]/, '')
        name = name.prepend(namespace + ':') if namespace.present?
        channel.queue(name, durable: true, arguments: arguments)
      end
    end

    # Return a mapping of queue names to the routing keys they're bound to.
    def bindings
      results = Hash.new { |hash, key| hash[key] = [] }
      api_client.bindings.each do |binding|
        next if binding['destination'] == binding['routing_key']
        next unless binding['source'] == @config[:mq_exchange]
        next unless binding['vhost'] == @config[:mq_vhost]
        results[binding['destination']] << binding['routing_key']
      end
      results
    end

    # Find the existing bindings, and unbind any redundant bindings
    def unbind_redundant_bindings(queue, routing_keys)
      return unless http_api_use_enabled?

      bindings.each do |dest, keys|
        next unless dest == queue.name
        keys.reject { |key| routing_keys.include?(key) }.each do |key|
          logger.debug "removing redundant binding #{queue.name} <--> #{key}"
          queue.unbind(exchange, routing_key: key)
        end
      end
    end

    # Bind a queue to the broker's exchange on the routing keys provided. Any
    # existing bindings on the queue that aren't present in the array of
    # routing keys will be unbound.
    def bind_queue(queue, routing_keys)
      unbind_redundant_bindings(queue, routing_keys)

      # Ensure all the desired bindings are present
      routing_keys.each do |routing_key|
        logger.debug "creating binding #{queue.name} <--> #{routing_key}"
        queue.bind(exchange, routing_key: routing_key)
      end
    end

    def stop
      if defined?(JRUBY_VERSION)
        channel.close
      else
        # Enqueue a failing job that kills the consumer loop
        channel_work_pool.shutdown
        # Give `timeout` seconds to jobs that are still being processed
        channel_work_pool.join(@config[:graceful_exit_timeout])
        # If after `timeout` they are still running, they are killed
        channel_work_pool.kill
      end
    end

    def requeue(delivery_tag)
      channel.reject(delivery_tag, true)
    end

    def reject(delivery_tag, requeue=false)
      channel.reject(delivery_tag, requeue)
    end

    def ack(delivery_tag)
      channel.ack(delivery_tag, false)
    end

    def nack(delivery_tag)
      channel.nack(delivery_tag, false, false)
    end

    def publish(*args)
      @publisher.publish(*args)
    end

    def publish_wait(*args)
      @publisher.publish_wait(*args)
    end

    def confirm_select(*args)
      channel.confirm_select(*args)
    end

    def wait_for_confirms
      channel.wait_for_confirms
    end

    # @return [Boolean] True if channel is set up to use publisher confirmations.
    def using_publisher_confirmations?
      channel.using_publisher_confirmations?
    end

    private

    def channel_broker
      Thread.current[CHANNEL_BROKER_KEY] ||= ChannelBroker.new(@connection, @config)
    end

    def api_config
      @api_config ||= OpenStruct.new.tap do |config|
        config.host = @config[:mq_api_host]
        config.port = @config[:mq_api_port]
        config.username = @config[:mq_username]
        config.password = @config[:mq_password]
        config.ssl = @config[:mq_api_ssl]
        config.protocol = config.ssl ? 'https://' : 'http://'
        config.sanitized_uri = "#{config.protocol}#{config.username}@#{config.host}:#{config.port}/"
      end
    end

    def connection_params
      parse_uri

      {}.tap do |params|
        params[:host]               = @config[:mq_host]
        params[:port]               = @config[:mq_port]
        params[:vhost]              = @config[:mq_vhost].presence || Hutch::Adapter::DEFAULT_VHOST
        params[:username]           = @config[:mq_username]
        params[:password]           = @config[:mq_password]
        params[:tls]                = @config[:mq_tls]
        params[:tls_key]            = @config[:mq_tls_key]
        params[:tls_cert]           = @config[:mq_tls_cert]
        params[:verify_peer]        = @config[:mq_verify_peer]
        if @config[:mq_tls_ca_certificates]
          params[:tls_ca_certificates] = @config[:mq_tls_ca_certificates]
        end
        params[:heartbeat]          = @config[:heartbeat]
        params[:connection_timeout] = @config[:connection_timeout]
        params[:read_timeout]       = @config[:read_timeout]
        params[:write_timeout]      = @config[:write_timeout]

        params[:automatically_recover] = true
        params[:network_recovery_interval] = 1

        params[:logger] = @config[:client_logger] if @config[:client_logger]
      end
    end

    def parse_uri
      return if @config[:uri].blank?

      u = URI.parse(@config[:uri])

      @config[:mq_tls]      = u.scheme == 'amqps'
      @config[:mq_host]     = u.host
      @config[:mq_port]     = u.port || default_mq_port
      @config[:mq_vhost]    = u.path.sub(%r{^/}, '')
      @config[:mq_username] = u.user
      @config[:mq_password] = u.password
    end

    def default_mq_port
      @config[:mq_tls] ? DEFAULT_AMQPS_PORT : DEFAULT_AMQP_PORT
    end

    def sanitized_uri
      p = connection_params
      scheme = p[:tls] ? 'amqps' : 'amqp'

      "#{scheme}://#{p[:username]}@#{p[:host]}:#{p[:port]}/#{p[:vhost].sub(%r{^/}, '')}"
    end

    def channel_work_pool
      channel.work_pool
    end
  end
end
