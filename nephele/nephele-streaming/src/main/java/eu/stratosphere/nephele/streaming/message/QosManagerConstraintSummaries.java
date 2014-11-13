package eu.stratosphere.nephele.streaming.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosManagerID;

public class QosManagerConstraintSummaries extends AbstractSerializableQosMessage {
	
	private List<QosConstraintSummary> constraintSummaries;
	
	private QosManagerID qosManagerID;

	private long timestamp;

	public QosManagerConstraintSummaries() {
	}
	
	public QosManagerConstraintSummaries(JobID jobID,
			QosManagerID qosManagerID, long timestamp,
			List<QosConstraintSummary> constraintSummaries) {
		
		super(jobID);
		this.qosManagerID = qosManagerID;
		this.timestamp = timestamp;
		this.constraintSummaries = constraintSummaries;
	}
	
	public List<QosConstraintSummary> getConstraintSummaries() {
		return constraintSummaries;
	}

	public QosManagerID getQosManagerID() {
		return qosManagerID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		qosManagerID.write(out);
		out.writeLong(timestamp);
		out.writeInt(constraintSummaries.size());
		for (QosConstraintSummary summary : constraintSummaries) {
			summary.write(out);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		qosManagerID = new QosManagerID();
		qosManagerID.read(in);
		timestamp = in.readLong();

		constraintSummaries = new LinkedList<QosConstraintSummary>();
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			QosConstraintSummary summary = new QosConstraintSummary();
			constraintSummaries.add(summary);
			summary.read(in);
		}
	}
}
