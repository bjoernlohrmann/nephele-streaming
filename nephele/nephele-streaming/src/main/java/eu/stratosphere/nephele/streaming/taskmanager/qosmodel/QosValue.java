package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;


/**
 * A Qos value represents a set of measurements ("samples") of some runtime
 * aspect, e.g. a set of vertex latency measurements. A Qos value holds a timestam, the mean
 * and optionally the variance of these measurements/samples.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosValue implements Comparable<QosValue> {

	private final long timestamp;
	
	private final double mean;
	
	private final double variance;

	private int weight;

	public QosValue(double mean, long timestamp) {
		this(mean, 1, timestamp);
	}
	
	public QosValue(double mean, int weight, long timestamp) {
		this.mean = mean;
		this.variance = -1;
		this.weight = weight;
		this.timestamp = timestamp;
	}
	
	public QosValue(double mean, double variance, int weight, long timestamp) {
		this.mean = mean;
		this.variance = variance;
		this.weight = weight;
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return this.timestamp;
	}
	
	public double getMean() {
		return this.mean;
	}

	public double getVariance() {
		return this.variance;
	}
	
	public boolean hasVariance() {
		return this.variance != -1;
	}

	public int getWeight() {
		return weight;
	}

	/**
	 * Sorts first by mean and then by timestamp.
	 */
	@Override
	public int compareTo(QosValue other) {
		return new CompareToBuilder()
			.append(this.mean, other.mean)
		 	.append(this.variance, other.variance)
		 	.append(this.timestamp, other.timestamp)
		 	.toComparison();		
	}

	@Override
	public boolean equals(Object otherObj) {
		if (otherObj instanceof QosValue) {
			QosValue other = (QosValue) otherObj;
			return new EqualsBuilder()
				.append(this.mean, other.mean)
				.append(this.variance, other.variance)
				.append(this.timestamp, other.timestamp)
				.isEquals();
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return Long.valueOf(this.timestamp).hashCode();
	}
}
