package eu.stratosphere.nephele.streaming.taskmanager.runtime.io;

import java.util.ArrayList;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosStatistic;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosValue;
import eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool;

/**
 * Installed at the gate to allow local, fine grained control over channel
 * output buffer sizes. Once every (configurable) adjustment interval, the
 * output buffer size of a channel will be chosen, so that it can reach a given
 * output buffer latency. While QosManagers choose a target output buffer
 * latency for each channel, this class will locally enforce the targets as good
 * as it can.
 */
public class OutputBufferLatencyEnforcer {
	
	private static final String INITIAL_TARGET_OBL_KEY = "plugins.streaming.taskmanager.io.initialTargetOutputBufferLatencyMillis";

	private static final int INITIAL_TARGET_OBL_DEFAULT = 10;

	private static final String ADJUSTMENT_INTERVAL_KEY = "plugins.streaming.taskmanager.io.outputBufferSizeAdjustmentIntervalMillis";

	private static final int ADJUSTMENT_INTERVAL_DEFAULT = 2000;

	private ArrayList<ChannelStatistics> channelStats = new ArrayList<ChannelStatistics>();

	private final int maxOutputBufferSize;
	
	private final int adjustmentIntervalMillis;

	private class ChannelStatistics {
		
		QosStatistic oblStatistic = new QosStatistic(5);

		int outputBuffersSentSinceLastAdjust = 0;

		int recordsEmittedSinceLastAdjust = 0;
		
		public long totalBytesTransmitted = 0;
		
		public long totalBytesTransmittedAtLastAdjust = 0;

		private int targetOutputBufferLatency;

		private long timeOfLastAdjust = -1;

		private int outputBufferSize = maxOutputBufferSize;

		private int adjustFrequency = 1; // in number of output buffers

		private final AbstractByteBufferedOutputChannel<?> channel;

		ChannelStatistics(AbstractByteBufferedOutputChannel<?> channel) {
			this.channel = channel;
			this.targetOutputBufferLatency = GlobalConfiguration.getInteger(
					INITIAL_TARGET_OBL_KEY, INITIAL_TARGET_OBL_DEFAULT);
						
		}
		
		void adjustOutputBufferSize() {
			long now = System.currentTimeMillis();
			int newOutputBufferSize = computeNewOutputBufferSize(now);
			
			this.channel.limitBufferSize(newOutputBufferSize);
			this.outputBufferSize = newOutputBufferSize;

			resetCounters(now);
		}

		private void resetCounters(long now) {
			// recalibrate adjust frequency
			double buffersPerMilli = outputBuffersSentSinceLastAdjust
					/ ((double) now - timeOfLastAdjust);

			this.adjustFrequency = (int) Math.max(1, adjustmentIntervalMillis * buffersPerMilli);

			this.outputBuffersSentSinceLastAdjust = 0;
			this.recordsEmittedSinceLastAdjust = 0;
			this.totalBytesTransmittedAtLastAdjust = totalBytesTransmitted;
			this.timeOfLastAdjust = now;
		}

		private int computeNewOutputBufferSize(long now) {
			double millisPassed = now - timeOfLastAdjust;
			double currObl = millisPassed / outputBuffersSentSinceLastAdjust / 2;
			oblStatistic.addValue(new QosValue(currObl, now));
			
			int newOutputBufferSize = (int) (outputBufferSize * (targetOutputBufferLatency / oblStatistic
					.getArithmeticMean()));
			
			
			long bytesTransmittedSinceLastAdjust = totalBytesTransmitted - totalBytesTransmittedAtLastAdjust; 
			double avgRecordSize = ((double) bytesTransmittedSinceLastAdjust) / recordsEmittedSinceLastAdjust;

			return (int) Math.max(avgRecordSize,
					Math.min(newOutputBufferSize, maxOutputBufferSize));
		}

		void adjustOutputBufferSizeIfDue() {
			if (timeOfLastAdjust == -1) {
				timeOfLastAdjust = System.currentTimeMillis();
				return;
			}

			if (outputBuffersSentSinceLastAdjust >= adjustFrequency
					&& recordsEmittedSinceLastAdjust>0) {
				this.adjustOutputBufferSize();
			}
		}

		void setTargetOutputBufferLatency(int targetOutputBufferLatency) {
			this.targetOutputBufferLatency = targetOutputBufferLatency;
			resetCounters(System.currentTimeMillis());
		}
	}

	public OutputBufferLatencyEnforcer() {
		this.maxOutputBufferSize = GlobalConfiguration.getInteger(
				"channel.network.bufferSizeInBytes",
				GlobalBufferPool.DEFAULT_BUFFER_SIZE_IN_BYTES);
		this.adjustmentIntervalMillis = GlobalConfiguration.getInteger(
				ADJUSTMENT_INTERVAL_KEY, ADJUSTMENT_INTERVAL_DEFAULT);
	}
	
	public void addOutputChannel(AbstractByteBufferedOutputChannel<?> channel) {
		if (this.channelStats.size() != channel.getChannelIndex()) {
			throw new RuntimeException("Wring Channel index. This is a bug");
		}

		this.channelStats.add(new ChannelStatistics(channel));
	}

	public void outputBufferSent(int channelIndex, long currAmountTransmitted) {
		ChannelStatistics stats = this.channelStats.get(channelIndex); 
		stats.outputBuffersSentSinceLastAdjust++;
		stats.totalBytesTransmitted =  currAmountTransmitted;
		stats.adjustOutputBufferSizeIfDue();
	}

	public void reportRecordEmitted(int channelIndex) {
		this.channelStats.get(channelIndex).recordsEmittedSinceLastAdjust++;
	}

	public void setTargetOutputBufferLatency(int channelIndex,
			int targetOutputBufferLatency) {
		this.channelStats.get(channelIndex).setTargetOutputBufferLatency(
				targetOutputBufferLatency);
	}
}
