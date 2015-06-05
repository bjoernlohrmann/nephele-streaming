package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling;

public interface Sampler {
	boolean hasSample();

	Sample drawSampleAndReset(long now);
}
