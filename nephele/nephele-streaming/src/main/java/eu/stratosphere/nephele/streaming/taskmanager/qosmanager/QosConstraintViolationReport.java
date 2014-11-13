package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;

public class QosConstraintViolationReport implements IOReadableWritable {

	private LatencyConstraintID constraintID;

	private long latencyConstraintMillis;

	private double minSequenceLatency = Double.MAX_VALUE;;

	private double maxSequenceLatency = Double.MIN_VALUE;

	private double aggSequenceLatency = 0;

	private int noOfSequences = 0;

	private int noOfSequencesBelowConstraint = 0;

	private int noOfSequencesAboveConstraint = 0;

	private boolean isFinalized = false;
	
	public QosConstraintViolationReport() {
	}

	public QosConstraintViolationReport(JobGraphLatencyConstraint constraint) {
		this(constraint.getID(), constraint.getLatencyConstraintInMillis());
	}

	public QosConstraintViolationReport(LatencyConstraintID constraintID,
			long latencyConstraintMillis) {

		this.constraintID = constraintID;
		this.latencyConstraintMillis = latencyConstraintMillis;
	}

	public LatencyConstraintID getLatencyConstraintID() {
		return constraintID;
	}

	public long getLatencyConstraintMillis() {
		return latencyConstraintMillis;
	}

	public LatencyConstraintID getConstraintID() {
		return constraintID;
	}

	public double getMinSequenceLatency() {
		return minSequenceLatency;
	}

	public double getMeanSequenceLatency() {
		ensureIsFinalized();
		return aggSequenceLatency;
	}

	public double getMaxSequenceLatency() {
		return maxSequenceLatency;
	}

	public int getNoOfSequences() {
		return noOfSequences;
	}

	public int getNoOfSequencesBelowConstraint() {
		return noOfSequencesBelowConstraint;
	}

	public int getNoOfSequencesAboveConstraint() {
		return noOfSequencesAboveConstraint;
	}

	public void addQosSequenceLatencySummary(
			QosSequenceLatencySummary sequenceSummary) {

		if (this.isFinalized) {
			throw new RuntimeException(
					"Cannot add sequence to already finalized summary. This is a bug.");
		}

		double sequenceLatency = sequenceSummary.getSequenceLatency();
		this.aggSequenceLatency += sequenceLatency;

		if (sequenceLatency < this.minSequenceLatency) {
			this.minSequenceLatency = sequenceLatency;
		}

		if (sequenceLatency > this.maxSequenceLatency) {
			this.maxSequenceLatency = sequenceLatency;
		}

		this.noOfSequences++;

		double constraintViolatedByMillis = sequenceLatency
				- latencyConstraintMillis;

		// only count violations of >5% of the constraint
		if (Math.abs(constraintViolatedByMillis) / latencyConstraintMillis > 0.05) {
			if (constraintViolatedByMillis > 0) {
				this.noOfSequencesAboveConstraint++;
			} else {
				this.noOfSequencesBelowConstraint++;
			}
		}
	}
	
	public void merge(QosConstraintViolationReport other) {
		if(!constraintID.equals(other.constraintID)) {
			throw new RuntimeException("Cannot merge violation reports belonging to different constraints. This is bug.");
		}
		
		if (this.isFinalized) {
			throw new RuntimeException(
					"Cannot merge with into a finalized summary. This is a bug.");
		}
		
		
		this.aggSequenceLatency += other.noOfSequences * other.aggSequenceLatency;

		if (other.minSequenceLatency < this.minSequenceLatency) {
			this.minSequenceLatency = other.minSequenceLatency;
		}

		if (other.maxSequenceLatency > this.maxSequenceLatency) {
			this.maxSequenceLatency = other.maxSequenceLatency;
		}

		this.noOfSequences += other.noOfSequences;
	}

	private void ensureIsFinalized() {
		if (!this.isFinalized) {
			if (this.noOfSequences > 0) {
				this.aggSequenceLatency /= this.noOfSequences;
			} else {
				this.aggSequenceLatency = 0;
				this.minSequenceLatency = 0;
				this.maxSequenceLatency = 0;
				this.noOfSequencesAboveConstraint = 0;
				this.noOfSequencesBelowConstraint = 0;
			}
		}
		this.isFinalized = true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		ensureIsFinalized();

		this.constraintID.write(out);
		out.writeLong(latencyConstraintMillis);
		out.writeDouble(minSequenceLatency);
		out.writeDouble(maxSequenceLatency);
		out.writeDouble(aggSequenceLatency);
		out.writeInt(noOfSequences);
		out.writeInt(noOfSequencesAboveConstraint);
		out.writeInt(noOfSequencesBelowConstraint);
	}

	@Override
	public void read(DataInput in) throws IOException {
		constraintID = new LatencyConstraintID();
		constraintID.read(in);
		latencyConstraintMillis = in.readLong();

		this.minSequenceLatency = in.readDouble();
		this.maxSequenceLatency = in.readDouble();
		this.aggSequenceLatency = in.readDouble();
		this.noOfSequences = in.readInt();
		this.noOfSequencesAboveConstraint = in.readInt();
		this.noOfSequencesBelowConstraint = in.readInt();
		this.isFinalized = true;
	}
}
