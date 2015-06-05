package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling;

/**
 * Samples the elapsed time between a read on a specific input gate identified
 * and the beginning of the next attempt read on any other input gate. Elapsed time is sampled in
 * microseconds.
 */
public class InputGateInterReadTimeSampler implements Sampler {

	/**
	 * Samples the elapsed time between a read on the input gate and the next read on any other input gate.
	 * Elapsed time is sampled in microseconds.
	 */
	private final BernoulliSampler readReadTimeSampler;

	private long lastSampleReadTime;

	public InputGateInterReadTimeSampler(double samplingProbability) {
		readReadTimeSampler = new BernoulliSampler(samplingProbability);
		lastSampleReadTime = -1;
	}
	
	public void recordReceivedOnIg() {
		if (readReadTimeSampler.shouldTakeSamplePoint()) {
			lastSampleReadTime = System.nanoTime();
		}
	}
	
	public void tryingToReadRecordFromAnyIg() {
		if (lastSampleReadTime != -1) {
			// if lastSampleReadTime is set then we should sample
			readReadTimeSampler.addSamplePoint((System.nanoTime() - lastSampleReadTime) / 1000.0);
			lastSampleReadTime = -1;
		}
	}
	
	public boolean hasSample() {
		return readReadTimeSampler.hasSample();
	}

	public Sample drawSampleAndReset(long now) {
		return readReadTimeSampler.drawSampleAndReset(now);
	}
}
