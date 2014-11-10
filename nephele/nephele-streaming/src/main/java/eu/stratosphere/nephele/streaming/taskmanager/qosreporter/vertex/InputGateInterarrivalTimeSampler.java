package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.BernoulliSampler;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.Sample;
import eu.stratosphere.nephele.streaming.util.StreamUtil;

public class InputGateInterarrivalTimeSampler {
	
	/**
	 * Samples records interarrival times in microseconds. These are computed
	 * from buffer interarrival times given in nanoseconds.
	 */
	private final BernoulliSampler interarrivalTimeSampler;
	
	private Long[] accBufferInterarrivalTimes = new Long[0];

	public InputGateInterarrivalTimeSampler(double samplingProbability) {
		this.interarrivalTimeSampler = new BernoulliSampler(samplingProbability);
	}

	public void inputBufferConsumed(int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
		
		if (accBufferInterarrivalTimes.length <= channelIndex) {
			accBufferInterarrivalTimes = StreamUtil.setInArrayAt(
					accBufferInterarrivalTimes, Long.class, channelIndex, null);
		}

		Long channelAccumulatedTime = accBufferInterarrivalTimes[channelIndex];

		if (channelAccumulatedTime == null && recordsReadFromBuffer > 0) {
			startSampleIfNecessary(channelIndex);

		} else if (channelAccumulatedTime != null) {
			finalizeOrAccumulateSamplePoint(channelIndex,
					bufferInterarrivalTimeNanos, recordsReadFromBuffer);
		}
	}

	private void finalizeOrAccumulateSamplePoint(int channelIndex,
			long interarrivalTimeNanos, int recordsReadFromBuffer) {

		if (recordsReadFromBuffer > 0) {
			finalizeSamplePoint(channelIndex, interarrivalTimeNanos,
					recordsReadFromBuffer);
			startSampleIfNecessary(channelIndex);
		} else {
			accBufferInterarrivalTimes[channelIndex] += interarrivalTimeNanos;
		}
	}

	private void finalizeSamplePoint(int channelIndex,
			long interarrivalTimeNanos, int recordsReadFromBuffer) {
		
		Long channelAccumulatedTime = accBufferInterarrivalTimes[channelIndex];
		interarrivalTimeSampler.addSamplePoint((channelAccumulatedTime
				+ interarrivalTimeNanos) / 1000.0);
		for (int i = 0; i < recordsReadFromBuffer - 1; i++) {
			interarrivalTimeSampler.addSamplePoint(0);
		}
		accBufferInterarrivalTimes[channelIndex] = null;
	}

	private void startSampleIfNecessary(int channelIndex) {
		if (interarrivalTimeSampler.shouldTakeSamplePoint()) {
			accBufferInterarrivalTimes[channelIndex] = Long.valueOf(0);
		}
	}
	
	public boolean hasSample() {
		return interarrivalTimeSampler.hasSample();
	}
	
	public Sample drawSampleAndReset(long now) {
		return interarrivalTimeSampler.drawSampleAndReset(now);
	}
}
