package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;

public class Sample implements IOReadableWritable {

	private int samplingDurationMillis;
	private int noOfSamplePoints;
	private double mean;
	private double variance;

	public Sample() {
	}

	public Sample(int samplingDurationMillis, int noOfSamplePoints,
			double mean, double variance) {

		this.samplingDurationMillis = samplingDurationMillis;
		this.noOfSamplePoints = noOfSamplePoints;
		this.mean = mean;
		this.variance = variance;
	}

	public int getSamplingDurationMillis() {
		return samplingDurationMillis;
	}

	public double getMean() {
		return mean;
	}

	public double getVariance() {
		return variance;
	}
	
	public int getNoOfSamplePoints() {
		return noOfSamplePoints;
	}

	public Sample fuseWithDisjunctSample(Sample other) {

		int newSamplingDuration = samplingDurationMillis
				+ other.samplingDurationMillis;

		int newSamplePoints = noOfSamplePoints + other.noOfSamplePoints;

		double newMean = mean * (((double) noOfSamplePoints) / newSamplePoints);
		newMean += other.mean
				* (((double) other.noOfSamplePoints) / newSamplePoints);

		double ess = (noOfSamplePoints - 1) * variance
				+ (other.noOfSamplePoints - 1) * other.variance;
		double tgss = noOfSamplePoints * (mean - newMean) * (mean - newMean)
				+ other.noOfSamplePoints * (other.mean - newMean)
				* (other.mean - newMean);
		double newVariance = (ess / newSamplePoints) + (tgss / newSamplePoints);

		return new Sample(newSamplingDuration, newSamplePoints, newMean,
				newVariance);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(samplingDurationMillis);
		out.writeInt(noOfSamplePoints);
		out.writeDouble(mean);
		out.writeDouble(variance);
	}

	@Override
	public void read(DataInput in) throws IOException {
		samplingDurationMillis = in.readInt();
		noOfSamplePoints = in.readInt();
		mean = in.readDouble();
		variance = in.readDouble();
	}

	/**
	 * Rescales the mean and variance value. Produces a sample with new mean
	 * this.mean*factor and new variance this.variance* factor * factor.
	 * 
	 */
	public Sample rescale(double factor) {
		return new Sample(samplingDurationMillis, noOfSamplePoints, mean
				* factor, variance * factor * factor);
	}
}
