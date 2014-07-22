package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.SequenceQosSummary;

public class QosConstraintSummary {

	private JobGraphLatencyConstraint constraint;

	private double[][] aggregatedMemberLatencies;

	private double minTotalLatency;

	private double aggregatedTotalLatency;

	private double maxTotalLatency;

	private int noOfActiveMemberSequences;

	private boolean isFinalized;

	public QosConstraintSummary(JobGraphLatencyConstraint constraint) {
		this.constraint = constraint;
		JobGraphSequence jobGraphSequence = constraint.getSequence();

		this.aggregatedMemberLatencies = new double[jobGraphSequence.size()][];
		for (SequenceElement<JobVertexID> sequenceElement : jobGraphSequence) {
			int index = sequenceElement.getIndexInSequence();
			if (sequenceElement.isVertex()) {
				this.aggregatedMemberLatencies[index] = new double[] { 0 };
			} else {
				this.aggregatedMemberLatencies[index] = new double[] { 0, 0 };
			}
		}

		this.noOfActiveMemberSequences = 0;
		this.aggregatedTotalLatency = 0;
		this.minTotalLatency = Double.MAX_VALUE;
		this.maxTotalLatency = Double.MIN_VALUE;
	}

	public void addMemberSequenceSummary(SequenceQosSummary sequenceSummary) {
		if (this.isFinalized) {
			throw new RuntimeException(
					"Cannot add sequence to already finalized summary. This is a bug.");
		}

		double[][] memberLatencies = sequenceSummary.getMemberLatencies();
		for (int i = 0; i < memberLatencies.length; i++) {
			for (int j = 0; j < memberLatencies[i].length; j++) {
				this.aggregatedMemberLatencies[i][j] += memberLatencies[i][j];
			}
		}

		double sequenceLatency = sequenceSummary.getSequenceLatency();
		this.aggregatedTotalLatency += sequenceLatency;

		if (sequenceLatency < this.minTotalLatency) {
			this.minTotalLatency = sequenceLatency;
		}

		if (sequenceLatency > this.maxTotalLatency) {
			this.maxTotalLatency = sequenceLatency;
		}

		this.noOfActiveMemberSequences++;
	}

	private void ensureIsFinalized() {
		if (!this.isFinalized) {
			if (this.noOfActiveMemberSequences > 0) {
				this.aggregatedTotalLatency /= this.noOfActiveMemberSequences;
				for (int i = 0; i < this.aggregatedMemberLatencies.length; i++) {
					for (int j = 0; j < this.aggregatedMemberLatencies[i].length; j++) {
						this.aggregatedMemberLatencies[i][j] /= this.noOfActiveMemberSequences;
					}
				}
			} else {
				this.aggregatedTotalLatency = 0;
				this.minTotalLatency = 0;
				this.maxTotalLatency = 0;
			}
		}
		this.isFinalized = true;
	}

	public JobGraphLatencyConstraint getConstraint() {
		return constraint;
	}

	public double[][] getAvgSequenceMemberLatencies() {
		ensureIsFinalized();
		return aggregatedMemberLatencies;
	}

	public double getMinSequenceLatency() {
		ensureIsFinalized();
		return minTotalLatency;
	}

	public double getAvgSequenceLatency() {
		ensureIsFinalized();
		return aggregatedTotalLatency;
	}

	public double getMaxSequenceLatency() {
		ensureIsFinalized();
		return maxTotalLatency;
	}

	public int getNoOfActiveSequences() {
		ensureIsFinalized();
		return noOfActiveMemberSequences;
	}
}
