package eu.stratosphere.nephele.streaming.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;

public class QosManagerConstraintSummaries extends AbstractSerializableQosMessage {
	
	private List<QosConstraintSummary> constraintSummaries;

	public QosManagerConstraintSummaries() {
	}
	
	public QosManagerConstraintSummaries(JobID jobID, List<QosConstraintSummary> constraintSummaries) {
		super(jobID);
		this.constraintSummaries = constraintSummaries;
	}
	
	public List<QosConstraintSummary> getConstraintSummaries() {
		return constraintSummaries;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		
		out.writeInt(constraintSummaries.size());
		for (QosConstraintSummary summary : constraintSummaries) {
			summary.write(out);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		
		int toRead = in.readInt();
		this.constraintSummaries = new LinkedList<QosConstraintSummary>();
		for(int i=0; i< toRead; i++) {
			QosConstraintSummary summary = new QosConstraintSummary();
			this.constraintSummaries.add(summary);
			summary.read(in);
		}
	}
}
