package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

public class QosStatistic {

	private ArrayList<QosValue> sortedByValue;

	private LinkedList<QosValue> sortedById;

	private int statisticWindowSize;

	private int noOfStoredValues;

	private double sumOfValues;

	public QosStatistic(int statisticWindowSize) {
		this.sortedById = new LinkedList<QosValue>();
		this.sortedByValue = new ArrayList<QosValue>();
		this.statisticWindowSize = statisticWindowSize;
		clear();
	}
	
	public void clear() {
		this.noOfStoredValues = 0;
		this.sumOfValues = 0;
		this.sortedById.clear();
		this.sortedByValue.clear();
	}

	public void addValue(QosValue value) {
		QosValue droppedValue = this.insertIntoSortedByTimestamp(value);

		if (droppedValue != null) {
			this.removeFromSortedByValue(droppedValue);
			this.noOfStoredValues--;
			this.sumOfValues -= droppedValue.getValue();
		}

		this.insertIntoSortedByValue(value);
		this.noOfStoredValues++;
		this.sumOfValues += value.getValue();
	}

	private QosValue insertIntoSortedByTimestamp(QosValue value) {
		if (!this.sortedById.isEmpty()
				&& this.sortedById.getLast().getId() >= value.getId()) {
			throw new IllegalArgumentException(
					"Trying to add stale Qos statistic values. This should not happen.");
		}
		this.sortedById.add(value);

		if (this.noOfStoredValues >= this.statisticWindowSize) {
			return this.sortedById.removeFirst();
		}
		return null;
	}

	protected void insertIntoSortedByValue(QosValue value) {
		int insertionIndex = Collections
				.binarySearch(this.sortedByValue, value);
		if (insertionIndex < 0) {
			insertionIndex = -(insertionIndex + 1);
		}

		this.sortedByValue.add(insertionIndex, value);
	}

	protected void removeFromSortedByValue(QosValue toRemove) {
		int removeIndex = Collections
				.binarySearch(this.sortedByValue, toRemove);
		if (removeIndex < 0) {
			throw new IllegalArgumentException(
					"Trying to drop inexistant Qos statistic value. This should not happen.");
		}
		this.sortedByValue.remove(removeIndex);
	}

	public double getMedianValue() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot calculate median of empty value set");
		}

		int medianIndex = this.noOfStoredValues / 2;
		return this.sortedByValue.get(medianIndex).getValue();
	}

	public double getMaxValue() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot calculate the max value of empty value set");
		}
		return this.sortedByValue.get(this.noOfStoredValues - 1).getValue();
	}

	public double getMinValue() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot calculate the min value of empty value set");
		}
		return this.sortedByValue.get(0).getValue();
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

	public double getArithmeticMean() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot calculate the arithmetic mean of empty value set");
		}

		return this.sumOfValues / this.noOfStoredValues;
	}

	public boolean hasValues() {
		return this.noOfStoredValues > 0;
	}
}
