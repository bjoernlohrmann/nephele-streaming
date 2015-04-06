package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.edge;

import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.BernoulliSampleDesign;

/**
 * Created by bjoern on 4/6/15.
 */
public class OutputBufferLifetimeSampler {

	private final BernoulliSampleDesign sampleDesign;
	private int samples;
	private long outputBufferLifetimeSampleBeginTime;
	private long outputBufferLifetimeSampleSum;


	public OutputBufferLifetimeSampler(double samplingProbability) {
		sampleDesign = new BernoulliSampleDesign(samplingProbability);
		outputBufferLifetimeSampleBeginTime = -1;
		reset();
	}

	public void reset() {
		samples = 0;
		outputBufferLifetimeSampleSum = 0;
		sampleDesign.reset();
	}

	public void outputBufferSent() {
		if (outputBufferLifetimeSampleBeginTime != -1) {
			outputBufferLifetimeSampleSum += System.nanoTime() - outputBufferLifetimeSampleBeginTime;
			outputBufferLifetimeSampleBeginTime = -1;
			samples++;
		}
	}

	public void outputBufferAllocated() {
		if (sampleDesign.shouldSample()) {
			outputBufferLifetimeSampleBeginTime = System.nanoTime();
		}
	}

	public boolean hasSample() {
		return samples > 0;
	}


	public double getMeanOutputBufferLifetimeMillis() {
		return outputBufferLifetimeSampleSum / (1000000.0 * samples);
	}
}
