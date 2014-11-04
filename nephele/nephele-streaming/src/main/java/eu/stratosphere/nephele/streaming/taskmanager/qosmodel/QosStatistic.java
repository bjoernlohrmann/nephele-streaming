package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.util.LinkedList;

public class QosStatistic {

	private final LinkedList<QosValue> sortedById;

	private final int statisticWindowSize;

	private int noOfStoredValues;
	
	private int sumOfWeights;

	private double sumOfWeightedMeans;

	private double sumOfWeightedVariances;

	private final boolean hasVariance;
	
	private QosValue statisticCache;

	public QosStatistic(int statisticWindowSize) {
		this(statisticWindowSize, false);
	}

	public QosStatistic(int statisticWindowSize, boolean hasVariance) {
		this.sortedById = new LinkedList<QosValue>();
		this.statisticWindowSize = statisticWindowSize;
		this.hasVariance = hasVariance;
		clear();
	}

	public void clear() {
		this.noOfStoredValues = 0;
		this.sumOfWeightedMeans = 0;
		this.sumOfWeightedVariances = 0;
		this.sumOfWeights = 0;
		this.sortedById.clear();
	}

	public void addValue(QosValue value) {
		if (value.hasVariance() != hasVariance) {
			throw new RuntimeException(
					"Cannot put QosValues without variance into a statistic with variance, or vice versa");
		}

		QosValue droppedValue = this.insertIntoSortedByTimestamp(value);

		if (droppedValue != null) {
			this.noOfStoredValues--;
			this.sumOfWeights -= droppedValue.getWeight();
			this.sumOfWeightedMeans -= droppedValue.getWeight() * droppedValue.getMean();
			
			if (hasVariance) {
				this.sumOfWeightedVariances -= (droppedValue.getWeight() -1) * droppedValue.getVariance();
			}
		}

		this.noOfStoredValues++;
		this.sumOfWeights += value.getWeight();
		this.sumOfWeightedMeans += value.getWeight() * value.getMean();

		if (hasVariance) {
			this.sumOfWeightedVariances += (value.getWeight() -1) * value.getVariance();
		}
		
		this.statisticCache = null;
	}

	private QosValue insertIntoSortedByTimestamp(QosValue value) {
		if (!this.sortedById.isEmpty()
				&& this.sortedById.getLast().getTimestamp() >= value
						.getTimestamp()) {
			throw new IllegalArgumentException(
					"Trying to add stale Qos statistic values. This should not happen.");
		}
		this.sortedById.add(value);

		if (this.noOfStoredValues >= this.statisticWindowSize) {
			return this.sortedById.removeFirst();
		}
		return null;
	}

	public QosValue getOldestValue() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot get the oldest value of empty value set");
		}
		return this.sortedById.getFirst();
	}

	public QosValue getNewestValue() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot get the newest value of empty value set");
		}
		return this.sortedById.getLast();
	}

	public double getMean() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot calculate the arithmetic mean of empty value set");
		}
		
		if (statisticCache == null) {
			refreshStatistic();
		}

		return statisticCache.getMean();
	}

	private void refreshStatistic() {
		
		double mean;
		
		mean = sumOfWeightedMeans / sumOfWeights;
		
		if(Math.rint(sumOfWeightedMeans) == sumOfWeightedMeans) {
			mean = sumOfWeightedMeans / sumOfWeights;
		} else {			
			mean = 0;
			for(QosValue value : sortedById) {
				mean += (((double)value.getWeight()) / sumOfWeights) * (value.getMean() - mean);
			}
		}
		
		if (hasVariance()) { 
			double tgss = 0;
			for(QosValue value : sortedById) {
				double meanDiff = value.getMean() - mean;
				tgss += (meanDiff * meanDiff) * value.getWeight();
			}
			double variance = (sumOfWeightedVariances + tgss) / (sumOfWeights -1) ;
			
			statisticCache = new QosValue(mean, variance, sumOfWeights, getNewestValue().getTimestamp());
		} else {
			statisticCache = new QosValue(mean, sumOfWeights, getNewestValue().getTimestamp());
		}
	}

	public boolean hasVariance() {
		return this.hasVariance;
	}

	public double getVariance() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot calculate the variance of empty value set");
		}

		if (statisticCache == null) {
			refreshStatistic();
		}

		return statisticCache.getVariance();
	}

	public boolean hasValues() {
		return this.noOfStoredValues > 0;
	}
}
