require 'hutch/adapter'
require 'hutch/consumer'
require 'hutch/worker'
require 'hutch/broker'
require 'hutch/logging'
require 'hutch/serializers/identity'
require 'hutch/serializers/json'
require 'hutch/config'
require 'hutch/message'
require 'hutch/cli'
require 'hutch/version'
require 'hutch/error_handlers'
require 'hutch/exceptions'
require 'hutch/tracers'

module Hutch
  def self.register_consumer(consumer)
    self.consumers << consumer
  end

  def self.consumers
    @consumers ||= []
  end

  def self.logger
    Hutch::Logging.logger
  end

  class << self
    attr_writer :global_properties
  end

  def self.global_properties
    @global_properties ||= {}
  end

  # Connects to broker, if not yet connected.
  #
  # @param options [Hash] Connection options
  # @param config [Hash] Configuration
  # @option options [Boolean] :enable_http_api_use
  def self.connect(options = {}, config = Hutch::Config)
    return if connected?

    @broker = Hutch::Broker.new(config)
    @broker.connect(options)
  end

  def self.disconnect
    @broker.disconnect if @broker
  end

  class << self
    attr_reader :broker
  end

  # @return [Boolean]
  def self.connected?
    broker && broker.connection && broker.connection.open?
  end

  def self.publish(*args)
    broker.publish(*args)
  end

  def self.publish_wait(*args)
    broker.publish_wait(*args)
  end
end
