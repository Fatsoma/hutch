require 'spec_helper'
require 'hutch/channel_broker'

describe Hutch::ChannelBroker do
  before do
    Hutch::Config.initialize(client_logger: Hutch::Logging.logger)
    @config = Hutch::Config.to_hash
  end
  let!(:config) { @config }
  after do
    Hutch::Config.instance_variable_set(:@config, nil)
    Hutch::Config.initialize
  end
  let(:connection) { double('Connection') }
  let(:channel) { double('Channel') }
  subject(:channel_broker) { Hutch::ChannelBroker.new(connection, config) }

  shared_examples 'an empty channel broker' do
    %i[channel exchange default_wait_exchange wait_exchanges].each do |name|
      it { expect(channel_broker.instance_variable_get("@#{name}")).to be_nil }
    end
  end

  describe '#disconnect' do
    before do
      channel_broker.instance_variable_set('@channel', channel)
      allow(channel_broker).to receive(:active).and_return(active)
      allow(channel).to receive(:close)
    end

    subject! { channel_broker.disconnect }

    context 'when active' do
      let(:active) { true }

      it { expect(channel).to have_received(:close) }
      it_behaves_like 'an empty channel broker'
    end

    context 'when not active' do
      let(:active) { false }

      it { expect(channel).to_not have_received(:close) }
      it_behaves_like 'an empty channel broker'
    end
  end

  describe '#reconnect' do
    let(:new_channel) { double('Channel') }
    let(:new_exchange) { double('Exchange') }

    before do
      allow(channel_broker).to receive(:disconnect)
      allow(channel_broker).to receive(:open_channel!).and_return(new_channel)
      allow(channel_broker).to receive(:declare_exchange!).and_return(new_exchange)
    end

    subject! { channel_broker.reconnect }

    it { expect(channel_broker).to have_received(:disconnect) }
    it { expect(channel_broker).to have_received(:open_channel!) }
    it { expect(channel_broker).to have_received(:declare_exchange!) }
    it { is_expected.to eq(new_exchange) }
  end

  describe '#active' do
    before do
      channel_broker.instance_variable_set('@channel', channel)
    end

    subject { channel_broker.active }

    context 'when channel is active' do
      let(:active) { true }

      before { allow(channel).to receive(:active).and_return(active) }

      it { is_expected.to be true }
    end

    context 'when channel is not active' do
      let(:active) { false }

      before { allow(channel).to receive(:active).and_return(active) }

      it { is_expected.to be_falsey }
    end

    context 'when channel is nil' do
      let(:channel) { nil }
      let(:active) { true }

      it { is_expected.to be_falsey }
    end
  end

  describe '#declare_wait_exchange' do
    let(:expiration) { rand(1000..1_000_000) }
    let(:suffix) { expiration.to_s }
    let(:new_exchange) { double('Exchange') }
    let(:new_queue) { double('Queue') }
    let(:exchange_name) { "wait-exchange_#{suffix}" }
    let(:queue_name) { "wait-queue_#{suffix}" }
    let(:main_exchange_name) { 'main-exchange' }
    let(:queues) { channel_broker.channel.queues }

    before do
      channel_broker.instance_variable_set('@channel', channel)
      config[:mq_wait_exchange] = 'wait-exchange'
      config[:mq_wait_queue] = 'wait-queue'
      config[:mq_exchange] = main_exchange_name

      allow(channel).to receive(:active).and_return(true)
      allow(channel).to receive(:fanout).and_return(new_exchange)
      allow(channel).to receive(:queue).and_return(new_queue)
      allow(new_queue).to receive(:bind).and_return(new_queue)
    end

    subject! { channel_broker.declare_wait_exchange(expiration) }

    it do
      expect(channel).to have_received(:fanout).with(exchange_name, durable: true)
      expect(channel).to have_received(:queue)
        .with(queue_name,
              durable: true,
              arguments: { 'x-dead-letter-exchange' => main_exchange_name })
      expect(channel_broker.instance_variable_get(:@wait_exchanges)[suffix])
        .to eq(new_exchange)
      is_expected.to eq(new_exchange)
    end
  end

  describe '#open_channel' do
    let(:honey_badger) { double(:honey_badger) }
    let(:config) { {} }
    let(:method) { Class.new }

    before do
      stub_const('Honeybadger', honey_badger)
      allow(honey_badger).to receive(:add_breadcrumb)
      allow(honey_badger).to receive(:notify)
      allow(connection).to receive(:create_channel).and_return(channel)
      allow(connection).to receive(:prefetch_channel)
      allow(channel).to receive(:confirm_select)
      allow(channel).to receive(:on_error) do |&blk|
        @captured_block = blk
      end
    end

    subject { channel_broker.open_channel }

    context 'when no publisher_confirms or force_publisher_confirms option' do
      let(:config) { {} }

      it do
        is_expected.to eq(channel)
        expect(channel).to_not have_received(:confirm_select)
      end
    end

    context 'when publisher_confirms option' do
      let(:config) { { publisher_confirms: true } }

      it do
        is_expected.to eq(channel)
        expect(channel).to have_received(:confirm_select)
      end
    end

    context 'when force_publisher_confirms option' do
      let(:config) { { force_publisher_confirms: true } }

      it do
        is_expected.to eq(channel)
        expect(channel).to have_received(:confirm_select)
      end
    end

    context 'when on_error block is called' do
      let(:reply_code) { 406 }
      let(:reply_text) { 'delivery acknowledgement on channel 1 timed out' }
      let(:class_id) { 'test-class-id' }
      let(:method_id) { 'test-method-id' }

      subject do
        channel_broker.open_channel.tap do
          @captured_block.call(channel, method)
        end
      end

      context 'when method is AMQ::Protocol::Channel::Close' do
        let(:method) do
          AMQ::Protocol::Channel::Close.new(reply_code, reply_text, class_id, method_id)
        end
        let(:context) do
          {
            reply_code: reply_code,
            method: method.inspect
          }
        end

        it do
          is_expected.to eq(channel)
          expect(@captured_block).to be_a(Proc)
          expect(honey_badger).to have_received(:notify)
            .with(
              error_class: 'Hutch::ChannelBroker::OnError',
              error_message: reply_text,
              context: context
            )
        end
      end

      context 'when method is AMQ::Protocol::Channel::CloseOk' do
        let(:method) do
          AMQ::Protocol::Channel::CloseOk.new
        end
        let(:context) do
          {
            method: method.inspect
          }
        end

        it do
          is_expected.to eq(channel)
          expect(@captured_block).to be_a(Proc)
          expect(honey_badger).to have_received(:notify)
            .with(
              error_class: 'Hutch::ChannelBroker::OnError',
              error_message: 'Channel error',
              context: context
            )
        end
      end
    end
  end
end
