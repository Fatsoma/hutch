require 'hutch/waiter'

RSpec.describe Hutch::Waiter do
  let(:broker) { double }
  let(:instance) { described_class.new(broker) }
  let(:pid) { Process.pid }
  def start_kill_thread(signal)
    Thread.new do
      # sleep allows the worker time to reach IO select
      # before the kill signal is sent.
      sleep 0.1
      Process.kill signal, pid
    end
  end

  describe '#wait_until_signaled' do
    before { instance.register_handlers }

    context 'a QUIT signal is received', if: RSpec::Support::Ruby.mri? do
      it 'logs that hutch is stopping' do
        expect(Hutch::Logging.logger).to receive(:info)
          .with('caught SIGQUIT, stopping hutch...')

        start_kill_thread('QUIT')
        instance.wait_until_signaled
      end
    end

    context 'a TERM signal is received' do
      it 'logs that hutch is stopping' do
        expect(Hutch::Logging.logger).to receive(:info)
          .with('caught SIGTERM, stopping hutch...')

        start_kill_thread('TERM')
        instance.wait_until_signaled
      end
    end

    context 'a INT signal is received' do
      it 'logs that hutch is stopping' do
        expect(Hutch::Logging.logger).to receive(:info)
          .with('caught SIGINT, stopping hutch...')

        start_kill_thread('INT')
        instance.wait_until_signaled
      end
    end
  end

  describe '#acknowledge_error' do
    let(:delivery_info) do
      double('Delivery Info', routing_key: '', delivery_tag: 'dt')
    end
    let(:properties) { double('Properties', message_id: 'abc123') }

    subject { instance.acknowledge_error delivery_info, properties, StandardError.new }

    it 'stops when it runs a successful acknowledgement' do
      skip_ack = double handle: false
      always_ack = double handle: true
      never_used = double handle: true

      allow(instance).
        to receive(:error_acknowledgements).
        and_return([skip_ack, always_ack, never_used])

      expect(never_used).to_not receive(:handle)

      subject
    end

    it 'defaults to nacking' do
      skip_ack = double handle: false

      allow(instance).
        to receive(:error_acknowledgements).
        and_return([skip_ack, skip_ack])

      expect(broker).to receive(:nack)

      subject
    end
  end

  describe described_class::SHUTDOWN_SIGNALS do
    it 'includes only things in Signal.list.keys' do
      expect(described_class).to eq(described_class & Signal.list.keys)
    end
  end
end
