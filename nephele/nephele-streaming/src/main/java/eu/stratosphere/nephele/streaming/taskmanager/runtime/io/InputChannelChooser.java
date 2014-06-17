package eu.stratosphere.nephele.streaming.taskmanager.runtime.io;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class is used by the {@link StreamInputGate} to efficiently determine in
 * which order to read from the input channels that currently have input data
 * available. It implements a round-robin schedule over a permanently changing
 * set of available input channels.
 *
 * @author Bjoern Lohrmann
 */
public class InputChannelChooser {

	private final LinkedBlockingQueue<Integer> incomingInputAvailabilities = new LinkedBlockingQueue<Integer>();

	private final RoundRobinChannelSchedule channelSchedule = new RoundRobinChannelSchedule();

	private volatile boolean blockIfNoChannelAvailable = true;

	private int[] channelInputAvailibilityCounter;

	private int currentChannel;

	public InputChannelChooser() {
		this.channelInputAvailibilityCounter = new int[1];
		this.channelInputAvailibilityCounter[0] = 0;
		this.currentChannel = -1;
	}

	public boolean hasChannelAvailable() {
		this.dequeueIncomingAvailableChannels();
		return !this.blockIfNoChannelAvailable || !this.channelSchedule.isEmpty();
	}

	/**
	 * @return index of the next available channel, or -1 if no channel is
	 * currently available and blocking is switched off
	 * @throws InterruptedException if thread is interrupted while waiting.
	 */
	public int chooseNextAvailableChannel() throws InterruptedException {
		this.dequeueIncomingAvailableChannels();

		if (this.channelSchedule.isEmpty()) {
			this.waitForAvailableChannelsIfNecessary();
		}

		this.currentChannel = this.channelSchedule.nextChannel();
		return this.currentChannel;
	}

	public void setBlockIfNoChannelAvailable(boolean blockIfNoChannelAvailable) {
		this.blockIfNoChannelAvailable = blockIfNoChannelAvailable;
		synchronized (this.incomingInputAvailabilities) {
			// wake up any task thread that is waiting on available channels
			// so that it realizes it should be halted.
			this.incomingInputAvailabilities.notify();
		}
	}

	public void decreaseAvailableInputOnCurrentChannel() {
		this.channelInputAvailibilityCounter[this.currentChannel]--;
		if (this.channelInputAvailibilityCounter[this.currentChannel] == 0) {
			this.channelSchedule.unscheduleCurrentChannel();
		}
	}

	/**
	 * If blocking is switched on, this method blocks until at least one channel
	 * is available, otherwise it may return earlier. If blocking is switched
	 * off while a thread waits in this method, it will return earlier as well.
	 *
	 * @throws InterruptedException
	 */
	private void waitForAvailableChannelsIfNecessary()
			throws InterruptedException {

		synchronized (this.incomingInputAvailabilities) {
			while (this.incomingInputAvailabilities.isEmpty()
					&& this.blockIfNoChannelAvailable) {
				this.incomingInputAvailabilities.wait();
			}
		}
		this.dequeueIncomingAvailableChannels();
	}

	public void increaseAvailableInput(int channelIndex) {
		synchronized (this.incomingInputAvailabilities) {
			this.incomingInputAvailabilities.add(channelIndex);
			this.incomingInputAvailabilities.notify();
		}
	}

	private void dequeueIncomingAvailableChannels() {
		Integer channelIndex;
		while ((channelIndex = this.incomingInputAvailabilities.poll()) != null) {
			increaseChannelInputAvailability(channelIndex);
			this.channelSchedule.scheduleChannel(channelIndex);
		}
	}

	private void increaseChannelInputAvailability(int channelIndex) {
		if (channelIndex >= this.channelInputAvailibilityCounter.length) {
			int[] newAvailiblityCounters = new int[channelIndex + 1];
			System.arraycopy(this.channelInputAvailibilityCounter, 0,
					newAvailiblityCounters, 0,
					this.channelInputAvailibilityCounter.length);
			this.channelInputAvailibilityCounter = newAvailiblityCounters;
		}
		this.channelInputAvailibilityCounter[channelIndex]++;
	}

	public void setNoAvailableInputOnCurrentChannel() {
		this.channelInputAvailibilityCounter[this.currentChannel] = 0;
		this.channelSchedule.unscheduleCurrentChannel();
	}
}
