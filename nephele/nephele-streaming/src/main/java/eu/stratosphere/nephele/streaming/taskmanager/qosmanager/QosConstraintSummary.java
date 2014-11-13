package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;

public class QosConstraintSummary implements IOReadableWritable {

	private QosGroupElementSummary[] groupElemSummaries;

	private QosConstraintViolationReport violationReport;

	public QosConstraintSummary() {
	}

	public QosConstraintSummary(JobGraphLatencyConstraint constraint,
			QosConstraintViolationReport violationReport) {

		this.violationReport = violationReport;
		initGroupElemSummaryArray(constraint);
	}

	public void initGroupElemSummaryArray(JobGraphLatencyConstraint constraint) {
		JobGraphSequence seq = constraint.getSequence();
		groupElemSummaries = new QosGroupElementSummary[seq.size()];

		int i = 0;
		for (SequenceElement seqElem : seq) {
			if (seqElem.isVertex()) {
				groupElemSummaries[i] = new QosGroupVertexSummary();
			} else {
				groupElemSummaries[i] = new QosGroupEdgeSummary();
			}
			i++;
		}
	}

	public QosGroupElementSummary[] QosGroupElementSummaries() {
		return groupElemSummaries;
	}

	public QosConstraintViolationReport getViolationReport() {
		return violationReport;
	}

	public boolean doesSequenceStartWithVertex() {
		return this.groupElemSummaries[0].isVertex();
	}

	public int getSequenceLength() {
		return this.groupElemSummaries.length;
	}

	public LatencyConstraintID getLatencyConstraintID() {
		return this.violationReport.getConstraintID();
	}

	public QosGroupVertexSummary getGroupVertexSummary(int indexInSequence) {
		return (QosGroupVertexSummary) groupElemSummaries[indexInSequence];
	}

	public QosGroupEdgeSummary getGroupEdgeSummary(int indexInSequence) {
		return (QosGroupEdgeSummary) groupElemSummaries[indexInSequence];
	}

	public void merge(List<QosConstraintSummary> completeSummaries) {
		// merge violation reports
		for (QosConstraintSummary summary : completeSummaries) {
			violationReport.merge(summary.getViolationReport());
		}

		// merge elem summaries groupwise
		List<QosGroupElementSummary> elemSummaries = new LinkedList<QosGroupElementSummary>();
		for (int i = 0; i < getSequenceLength(); i++) {
			for (QosConstraintSummary toMerge : completeSummaries) {
				elemSummaries.add(toMerge.groupElemSummaries[i]);
			}
			groupElemSummaries[i].merge(elemSummaries);
			elemSummaries.clear();
		}
	}

	public boolean hasData() {
		for (QosGroupElementSummary elemSumary : groupElemSummaries) {
			if (!elemSumary.hasData()) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.violationReport.write(out);

		out.writeInt(getSequenceLength());
		out.writeBoolean(doesSequenceStartWithVertex());
		for (QosGroupElementSummary groupElem : groupElemSummaries) {
			groupElem.write(out);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		violationReport = new QosConstraintViolationReport();
		violationReport.read(in);

		int sequenceLength = in.readInt();
		boolean nextIsVertex = in.readBoolean();
		groupElemSummaries = new QosGroupElementSummary[sequenceLength];

		for (int i = 0; i < sequenceLength; i++) {
			if (nextIsVertex) {
				groupElemSummaries[i] = new QosGroupVertexSummary();
			} else {
				groupElemSummaries[i] = new QosGroupEdgeSummary();
			}
			groupElemSummaries[i].read(in);
			nextIsVertex = !nextIsVertex;
		}
	}
}
