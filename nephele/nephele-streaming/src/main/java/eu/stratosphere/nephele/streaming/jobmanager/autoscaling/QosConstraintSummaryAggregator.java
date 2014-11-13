package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintViolationReport;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupVertexSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosManagerID;

public class QosConstraintSummaryAggregator {
	
	private static final Log LOG = LogFactory.getLog(QosConstraintSummaryAggregator.class);
	
	private final HashMap<QosManagerID, QosConstraintSummary> summaries = new HashMap<QosManagerID, QosConstraintSummary>();
	
	private final Set<QosManagerID> requiredManagerIDs;

	private final ExecutionGraph executionGraph;

	private final JobGraphLatencyConstraint constraint;

	public QosConstraintSummaryAggregator(ExecutionGraph executionGraph, JobGraphLatencyConstraint constraint, Set<QosManagerID> managerIDs) {

		this.executionGraph = executionGraph;
		this.constraint = constraint;
		this.requiredManagerIDs = managerIDs;
	}
	
	public JobGraphLatencyConstraint getConstraint() {
		return constraint;
	}

	public void add(QosManagerID qosManagerID, QosConstraintSummary summary) {
		summaries.put(qosManagerID, summary);
	}
	
	public boolean canAggregate() {
		return this.summaries.size() == this.requiredManagerIDs.size();
	}
	
	public QosConstraintSummary computeAggregation() {
		QosConstraintViolationReport vioRep = new QosConstraintViolationReport(constraint);
		QosConstraintSummary aggregation = new QosConstraintSummary(constraint, vioRep);

		List<QosConstraintSummary> completeSummaries = getCompleteSummaries();
		aggregation.merge(completeSummaries);
		
		fixNoOfActiveVerticesAndEdges(aggregation);		
		return aggregation;
	}

	public void fixNoOfActiveVerticesAndEdges(QosConstraintSummary aggregation) {
		for(SequenceElement seqElem : constraint.getSequence()) {
			if(seqElem.isVertex()) {
				int trueNoOfSubtasks = this.executionGraph
						.getExecutionGroupVertex(seqElem.getVertexID())
						.getNumberOfRunningSubstasks();
				
				QosGroupVertexSummary vertexSum = aggregation.getGroupVertexSummary(seqElem.getIndexInSequence());	
				if(trueNoOfSubtasks > vertexSum.getActiveVertices()) {
					LOG.warn(String
							.format("Aggregation for \"%s\" does not provide enough data",
									seqElem.getName()));
				}
			
				vertexSum.setActiveVertices(trueNoOfSubtasks);
			} else {
				int trueNoOfEmitterSubtasks = this.executionGraph
						.getExecutionGroupVertex(seqElem.getSourceVertexID())
						.getNumberOfRunningSubstasks();

				int trueNoOfConsumerSubtasks = this.executionGraph
						.getExecutionGroupVertex(seqElem.getTargetVertexID())
						.getNumberOfRunningSubstasks();

				QosGroupEdgeSummary edgeSum = aggregation
						.getGroupEdgeSummary(seqElem.getIndexInSequence());
				
				if (trueNoOfEmitterSubtasks > edgeSum.getActiveEmitterVertices()
						|| trueNoOfConsumerSubtasks > edgeSum.getActiveConsumerVertices()) {
					
					LOG.warn(String
							.format("Aggregation for edge \"%s\" does not provide enough data",
									seqElem.getName() + seqElem.getIndexInSequence()));
				}
				
				edgeSum.setActiveEmitterVertices(trueNoOfEmitterSubtasks);
				edgeSum.setActiveConsumerVertices(trueNoOfConsumerSubtasks);
				edgeSum.setActiveEdges(-1);
			}
		}
	}

	private List<QosConstraintSummary> getCompleteSummaries() {
		List<QosConstraintSummary> ret = new LinkedList<QosConstraintSummary>();
		for (QosConstraintSummary summary : summaries.values()) {
			if (summary.hasData()) {
				ret.add(summary);
			}
		}
		return ret;
	}
}
