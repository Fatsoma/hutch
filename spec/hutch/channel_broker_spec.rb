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
    %i(channel exchange default_wait_exchange wait_exchanges).each do |name|
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
end
