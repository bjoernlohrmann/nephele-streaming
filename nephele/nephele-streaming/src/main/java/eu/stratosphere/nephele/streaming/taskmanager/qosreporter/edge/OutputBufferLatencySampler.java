package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.edge;

import eu.stratosphere.nephele.io.channels.bytebuffered.BufferFlushReason;

/**
 * Created by bjoern on 4/6/15.
 */
public class OutputBufferLatencySampler {

	private long sampleSum;
	private int samples;

	private int pendingSamples;
	private long pendingSampleSum;
	private long pendingIncompleteSample;
	private final long baseTime;

	public OutputBufferLatencySampler() {
		reset();
		this.pendingSamples = 0;
		this.pendingSampleSum = 0;
		this.pendingIncompleteSample = -1;
		this.baseTime = System.currentTimeMillis();
	}

	public void reset() {
		sampleSum = 0;
		samples = 0;
	}

	public void startSample(long nowTimestamp) {
		movePendingIncompleteSample();
		pendingIncompleteSample = nowTimestamp;
	}

	private void movePendingIncompleteSample() {
		if (pendingIncompleteSample != -1) {
			long toAdd = pendingIncompleteSample - baseTime;
			pendingSampleSum += toAdd;
			pendingSamples++;
			pendingIncompleteSample = -1;
		}
	}

	public void outputBufferSent(BufferFlushReason reason, long now) {
		if (reason == BufferFlushReason.AFTER_SERIALIZATION) {
			movePendingIncompleteSample();
		}

		if (pendingSamples > 0) {
			long toAdd = (pendingSamples * (now-baseTime)) - pendingSampleSum;

			sampleSum += toAdd;
			samples += pendingSamples;

			pendingSamples = 0;
			pendingSampleSum = 0;
		}
	}

	public boolean hasSample() {
		return samples > 0;
	}

	public double getMeanOutputBufferLatencyMillis() {
		return ((double) sampleSum) / samples;
	}
}
