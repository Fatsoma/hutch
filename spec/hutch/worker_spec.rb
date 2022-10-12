require 'spec_helper'
require 'hutch/worker'

describe Hutch::Worker do
  let(:consumer) { double('Consumer', routing_keys: %w( a b c ),
                          get_queue_name: 'consumer', get_arguments: {},
                          get_options: {}, get_serializer: nil) }
  let(:consumers) { [consumer, double('Consumer')] }
  let(:broker) { Hutch::Broker.new }
  let(:setup_procs) { Array.new(2) { proc {} } }
  let(:waiter) { worker.send(:waiter) }
  subject(:worker) { Hutch::Worker.new(broker, consumers, setup_procs) }

  describe ".#run" do
    it "calls each setup proc" do
      setup_procs.each { |prc| expect(prc).to receive(:call) }
      allow(waiter).to receive(:register_handlers)
      allow(worker).to receive(:setup_queues)
      allow(waiter).to receive(:wait_until_signaled)
      allow(broker).to receive(:stop)

      worker.run
    end
  end

  describe '#setup_queues' do
    it 'sets up queues for each of the consumers' do
      consumers.each do |consumer|
        expect(worker).to receive(:setup_queue).with(consumer)
      end
      worker.setup_queues
    end
  end

  describe '#setup_queue' do
    let(:queue) { double('Queue', bind: nil, subscribe: nil) }
    before { allow(broker).to receive_messages(queue: queue, bind_queue: nil) }

    it 'creates a queue' do
      expect(broker).to receive(:queue).with(consumer.get_queue_name, consumer.get_options).and_return(queue)
      worker.setup_queue(consumer)
    end

    it 'binds the queue to each of the routing keys' do
      expect(broker).to receive(:bind_queue).with(queue, %w( a b c ))
      worker.setup_queue(consumer)
    end

    it 'sets up a subscription' do
      expect(queue).to receive(:subscribe).with(consumer_tag: /^hutch\-.{36}$/, manual_ack: true)
      worker.setup_queue(consumer)
    end

    context 'with a configured consumer tag prefix' do
      before { Hutch::Config.set(:consumer_tag_prefix, 'appname') }

      it 'sets up a subscription with the configured tag prefix' do
        expect(queue).to receive(:subscribe).with(consumer_tag: /^appname\-.{36}$/, manual_ack: true)
        worker.setup_queue(consumer)
      end
    end

    context 'with a configured consumer tag prefix that is too long' do
      let(:maximum_size) { 255 - SecureRandom.uuid.size - 1 }
      before { Hutch::Config.set(:consumer_tag_prefix, 'a'.*(maximum_size + 1)) }

      it 'raises an error' do
        expect { worker.setup_queue(consumer) }.to raise_error(/Tag must be 255 bytes long at most/)
      end
    end
  end

  describe '#handle_message' do
    let(:payload) { '{}' }
    let(:consumer_instance) { double('Consumer instance') }
    let(:delivery_info) do
      double('Delivery Info', routing_key: '',
                              delivery_tag: 'dt')
    end
    let(:properties) { double('Properties', message_id: nil, content_type: 'application/json') }
    let(:handle_message) do
      worker.handle_message(consumer, delivery_info, properties, payload)
      waiter.handle_action(delivery_info.delivery_tag)
    end
    before { allow(consumer).to receive_messages(new: consumer_instance) }
    before { allow(broker).to receive(:ack) }
    before { allow(broker).to receive(:nack) }
    before { allow(consumer_instance).to receive(:broker=) }
    before { allow(consumer_instance).to receive(:delivery_info=) }
    before { waiter.register_handlers }
    after { Thread.main[:action_queue].clear }

    context 'when the consumer processes without an exception' do
      before { allow(consumer_instance).to receive(:process) }

      subject! { handle_message }

      it 'passes the message to the consumer' do
        expect(consumer_instance).to have_received(:process)
          .with(an_instance_of(Hutch::Message))
      end

      it 'acknowledges the message' do
        expect(broker).to have_received(:ack).with(delivery_info.delivery_tag)
      end
    end

    context 'when the consumer fails and a requeue is configured' do

      it 'requeues the message' do
        allow(consumer_instance).to receive(:process).and_raise('failed')
        requeuer = double
        allow(requeuer).to receive(:handle) { |delivery_info, properties, broker, e|
          broker.requeue delivery_info.delivery_tag
          true
        }
        allow(waiter).to receive(:error_acknowledgements).and_return([requeuer])
        expect(broker).to_not receive(:ack)
        expect(broker).to_not receive(:nack)
        expect(broker).to receive(:requeue)

        handle_message
      end
    end


    context 'when the consumer raises an exception' do
      before { allow(consumer_instance).to receive(:process).and_raise('a consumer error') }
      before do
        Hutch::Config[:error_handlers].each do |backend|
          allow(backend).to receive(:handle)
        end
      end

      subject! { handle_message }

      it 'logs the error' do
        Hutch::Config[:error_handlers].each do |backend|
          expect(backend).to have_received(:handle)
        end
      end

      it 'rejects the message' do
        expect(broker).to have_received(:nack).with(delivery_info.delivery_tag)
      end
    end

    context 'when the payload is not valid json' do
      let(:payload) { 'Not Valid JSON' }
      before do
        Hutch::Config[:error_handlers].each do |backend|
          allow(backend).to receive(:handle)
        end
      end

      subject! { handle_message }

      it 'logs the error' do
        Hutch::Config[:error_handlers].each do |backend|
          expect(backend).to have_received(:handle)
        end
      end

      it 'rejects the message' do
        expect(broker).to have_received(:nack).with(delivery_info.delivery_tag)
      end
    end
  end
end
