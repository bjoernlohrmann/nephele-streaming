package eu.stratosphere.nephele.streaming.taskmanager.runtime.io;

import java.util.ArrayList;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
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

	private static final int INITIAL_TARGET_OBL_DEFAULT = 50;

	private static final String ADJUSTMENT_INTERVAL_KEY = "plugins.streaming.taskmanager.io.outputBufferSizeAdjustmentIntervalMillis";

	private static final int ADJUSTMENT_INTERVAL_DEFAULT = 2000;

	private ArrayList<ChannelStatistics> channelStats = new ArrayList<ChannelStatistics>();

	private final int maxOutputBufferSize;
	
	private final int adjustmentIntervalMillis;

	private class ChannelStatistics {

		int recordsEmittedSinceLastAdjust = 0;
		
		public long amountTransmittedAtLastAdjust = 0;

		private int targetOutputBufferLatency;

		private long timeOfLastAdjust = -1;

		private int outputBufferSize = maxOutputBufferSize;
		
		private int adjustFrequency = 3; // in number of records

		private final AbstractByteBufferedOutputChannel<?> channel;

		ChannelStatistics(AbstractByteBufferedOutputChannel<?> channel) {
			this.channel = channel;
			this.targetOutputBufferLatency = GlobalConfiguration.getInteger(
					INITIAL_TARGET_OBL_KEY, INITIAL_TARGET_OBL_DEFAULT);
						
		}
		
		void adjustOutputBufferSize() {
			long now = System.currentTimeMillis();
			
			long millisPassedSinceLastAdjust = now - timeOfLastAdjust;
			if (millisPassedSinceLastAdjust < targetOutputBufferLatency) {
				return;
			}
			
			int newOutputBufferSize = computeNewOutputBufferSize(millisPassedSinceLastAdjust);
			
			this.channel.limitBufferSize(newOutputBufferSize);
			int oldOutputBufferSize = this.outputBufferSize; 
			this.outputBufferSize = newOutputBufferSize;

			resetCounters(now, oldOutputBufferSize);
		}

		private void resetCounters(long now, int oldOutputBufferSize) {
			// recalibrate adjust frequency
			double oldRecordsPerMilli = recordsEmittedSinceLastAdjust
					/ ((double) now - timeOfLastAdjust);

			this.adjustFrequency = (int) Math.max(1, adjustmentIntervalMillis * oldRecordsPerMilli);

			this.recordsEmittedSinceLastAdjust = 0;
			this.amountTransmittedAtLastAdjust = channel
					.getAmountOfDataTransmitted();
			this.timeOfLastAdjust = now;
		}

		private int computeNewOutputBufferSize(long millisPassedSinceLastAdjust) {
			
			double bytesSinceLastAdjust = channel.getAmountOfDataTransmitted()
					- amountTransmittedAtLastAdjust;
			
			double outputBuffersFilledSinceLastAdjust = bytesSinceLastAdjust / outputBufferSize;
			
			double currObl = millisPassedSinceLastAdjust / outputBuffersFilledSinceLastAdjust / 2;
			int newOutputBufferSize = (int) (outputBufferSize * (targetOutputBufferLatency / currObl));

			double avgRecordSize = Math.max(1, bytesSinceLastAdjust / recordsEmittedSinceLastAdjust);
			
//			System.err.printf("msPassed: %d | bytes: %d | #ob: %.2f | obl: %.1f | avgRecS: %d | oldObs: %d | newObs: %d\n",
//					millisPassedSinceLastAdjust,
//					(int) bytesSinceLastAdjust,
//					outputBuffersFilledSinceLastAdjust,
//					currObl,
//					(int) avgRecordSize,
//					outputBufferSize,
//					newOutputBufferSize);

			return (int) Math.max(avgRecordSize,
					Math.min(newOutputBufferSize, maxOutputBufferSize));
		}

		void adjustOutputBufferSizeIfDue() {
			if (timeOfLastAdjust == -1) {
				timeOfLastAdjust = System.currentTimeMillis();
				return;
			}

			if (recordsEmittedSinceLastAdjust >= adjustFrequency) {
				this.adjustOutputBufferSize();
			}
		}

		void setTargetOutputBufferLatency(int targetOutputBufferLatency) {
			this.targetOutputBufferLatency = targetOutputBufferLatency;
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

	public void reportRecordEmitted(int channelIndex) {
		ChannelStatistics stats = this.channelStats.get(channelIndex);
		stats.recordsEmittedSinceLastAdjust++;
		stats.adjustOutputBufferSizeIfDue();
	}

	public void setTargetOutputBufferLatency(int channelIndex,
			int targetOutputBufferLatency) {
		this.channelStats.get(channelIndex).setTargetOutputBufferLatency(
				targetOutputBufferLatency);
	}
}
