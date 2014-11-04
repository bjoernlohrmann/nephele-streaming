package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGate;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupEdge;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;

/**
 * This class holds factory methods to build Qos graphs.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosGraphFactory {

	/**
	 * Builds the smallest possible subgraph of the given execution graph, where
	 * all vertices and edges are affected by the given constraint. If the
	 * constraint starts/ends with and edge the respective source/target vertex
	 * is also part of the QosGraph, although it is not strictly part of the
	 * constraint.
	 * 
	 * @param execGraph
	 *            An execution graph.
	 * @param constraint
	 *            A latency constraint the affects elements of the given
	 *            execution graph.
	 * 
	 * @return A {@link QosGraph} that contains all those vertices and edges of
	 *         the given execution graph, that are covered by the given latency
	 *         constraint.
	 */
	public static QosGraph createConstrainedQosGraph(ExecutionGraph execGraph,
			JobGraphLatencyConstraint constraint) {

		QosGroupVertex startVertex = null;
		ExecutionGroupVertex currExecVertex = null;
		QosGroupVertex currGroupVertex = null;

		for (SequenceElement sequenceElem : constraint
				.getSequence()) {

			if (currExecVertex == null) {
				JobVertexID firstJobVertexID;

				if (sequenceElem.isVertex()) {
					firstJobVertexID = sequenceElem.getVertexID();
				} else {
					firstJobVertexID = sequenceElem.getSourceVertexID();
				}
				currExecVertex = findGroupVertex(execGraph, firstJobVertexID);
				currGroupVertex = toQosGroupVertex(currExecVertex);
				startVertex = currGroupVertex;
			}

			if (sequenceElem.isEdge()) {
				ExecutionGroupEdge execEdge = currExecVertex
						.getForwardEdge(sequenceElem.getOutputGateIndex());

				ExecutionGroupVertex nextExecVertex = execEdge
						.getTargetVertex();
				QosGroupVertex nextGroupVertex = toQosGroupVertex(nextExecVertex);

				wireTo(currGroupVertex, nextGroupVertex, execEdge);
				currExecVertex = nextExecVertex;
				currGroupVertex = nextGroupVertex;
			}
		}

		return new QosGraph(startVertex, constraint);
	}

	private static void wireTo(QosGroupVertex from, QosGroupVertex to,
			ExecutionGroupEdge execEdge) {

		int outputGate = execEdge.getIndexOfOutputGate();
		int inputGate = execEdge.getIndexOfInputGate();

		QosGroupEdge qosEdge = new QosGroupEdge(
				execEdge.getDistributionPattern(), from, to, outputGate,
				inputGate);

		connectGroupMembers(qosEdge, execEdge);
	}

	private static QosGroupVertex toQosGroupVertex(
			ExecutionGroupVertex execVertex) {

		QosGroupVertex qosGroupVertex = new QosGroupVertex(
				execVertex.getJobVertexID(), execVertex.getName());
		createGroupMembers(qosGroupVertex, execVertex);

		return qosGroupVertex;
	}

	private static ExecutionGroupVertex findGroupVertex(
			ExecutionGraph execGraph, JobVertexID jobVertexID) {

		ExecutionStage stage = execGraph.getStage(0);
		for (int i = 0; i < stage.getNumberOfStageMembers(); i++) {

			ExecutionGroupVertex stageMember = stage.getStageMember(i);
			if (stageMember.getJobVertexID().equals(jobVertexID)) {
				return stageMember;
			}
		}

		throw new RuntimeException(
				"Could not find execution group vertex for given job vertex id. This is a bug.");
	}

	/**
	 * Duplicates the execution edges and gates of the given execution group
	 * edge and links them to the given QoS group edge.
	 * 
	 */
	private static void connectGroupMembers(QosGroupEdge qosGroupEdge,
			ExecutionGroupEdge execGroupEdge) {

		int sourceMembers = execGroupEdge.getSourceVertex()
				.getCurrentNumberOfGroupMembers();

		for (int i = 0; i < sourceMembers; i++) {

			ExecutionGate execOutputGate = execGroupEdge.getSourceVertex()
					.getGroupMember(i)
					.getOutputGate(execGroupEdge.getIndexOfOutputGate());

			QosVertex sourceVertex = qosGroupEdge.getSourceVertex()
					.getMember(i);
			QosGate qosOutputGate = new QosGate(execOutputGate.getGateID(),
					qosGroupEdge.getOutputGateIndex());
			sourceVertex.setOutputGate(qosOutputGate);

			for (int j = 0; j < execOutputGate.getNumberOfEdges(); j++) {
				ExecutionEdge executionEdge = execOutputGate.getEdge(j);

				QosEdge qosEdge = new QosEdge(
						executionEdge.getOutputChannelID(),
						executionEdge.getInputChannelID(),
						executionEdge.getOutputGateIndex(),
						executionEdge.getInputGateIndex());

				QosVertex targetVertex = qosGroupEdge.getTargetVertex()
						.getMember(
								executionEdge.getInputGate().getVertex()
										.getIndexInVertexGroup());

				QosGate qosInputGate = targetVertex.getInputGate(qosGroupEdge
						.getInputGateIndex());
				if (qosInputGate == null) {
					qosInputGate = new QosGate(executionEdge.getInputGate()
							.getGateID(), qosGroupEdge.getInputGateIndex());
					targetVertex.setInputGate(qosInputGate);
				}

				qosEdge.setOutputGate(qosOutputGate);
				qosEdge.setInputGate(qosInputGate);
			}
		}
	}

	/**
	 * Populates qosGroupVertex with {@link QosVertex} objects, by duplicating
	 * the members found in executionGroupVertex.
	 * 
	 */
	private static void createGroupMembers(QosGroupVertex qosGroupVertex,
			ExecutionGroupVertex executionGroupVertex) {

		for (int i = executionGroupVertex.getCurrentNumberOfGroupMembers() - 1; i >= 0; i--) {
			ExecutionVertex executionVertex = executionGroupVertex
					.getGroupMember(i);

			qosGroupVertex.setGroupMember(QosVertex
					.fromExecutionVertex(executionVertex));
		}
	}

	public static QosGraph createConstrainedSubgraph(QosGraph qosGraph,
			LatencyConstraintID constraintID, List<QosVertex> anchors) {

		if (qosGraph.getConstraints().size() > 1) {
			throw new RuntimeException(
					"This method only works for QosGraphs with one constraint in them");
		}

		JobGraphSequence sequence = qosGraph.getConstraintByID(constraintID)
				.getSequence();
		final QosGraph toReturn = qosGraph.cloneWithoutMembers();
		QosGraphTraversalListener traversalListener = new QosGraphTraversalListener() {
			@Override
			public void processQosVertex(QosVertex vertex,
					SequenceElement sequenceElem) {
				// do nothing
			}

			@Override
			public void processQosEdge(QosEdge edge,
					SequenceElement sequenceElem) {
				addMembersAndMemberWiring(edge, toReturn);
			}
		};

		QosGraphTraversal traverser = new QosGraphTraversal(null, sequence,
				traversalListener);
		traverser.setClearTraversedVertices(false);
		for (QosVertex anchor : anchors) {
			traverser.setStartVertex(anchor);
			traverser.traverseForward();
			traverser.traverseBackward(false, true);
		}

		return toReturn;
	}

	private static void addMembersAndMemberWiring(QosEdge templateEdge,
			QosGraph clone) {
		// ensure source member vertex and output gate exist
		QosGate templateOutputGate = templateEdge.getOutputGate();
		QosVertex templateSourceVertex = templateOutputGate.getVertex();

		QosGroupVertex sourceGroupVertex = clone
				.getGroupVertexByID(templateSourceVertex.getGroupVertex()
						.getJobVertexID());
		QosVertex sourceVertex = sourceGroupVertex
				.getMember(templateSourceVertex.getMemberIndex());
		if (sourceVertex == null) {
			sourceVertex = templateSourceVertex.cloneWithoutGates();
			sourceGroupVertex.setGroupMember(sourceVertex);
		}
		QosGate outputGate = sourceVertex.getOutputGate(templateOutputGate
				.getGateIndex());
		if (outputGate == null) {
			outputGate = templateOutputGate.cloneWithoutEdgesAndVertex();
			sourceVertex.setOutputGate(outputGate);
		}

		// ensure source target vertex and output gate exist
		QosGate templateInputGate = templateEdge.getInputGate();
		QosVertex templateTargetVertex = templateInputGate.getVertex();

		QosGroupVertex targetGroupVertex = clone
				.getGroupVertexByID(templateTargetVertex.getGroupVertex()
						.getJobVertexID());
		QosVertex targetVertex = targetGroupVertex
				.getMember(templateTargetVertex.getMemberIndex());
		if (targetVertex == null) {
			targetVertex = templateTargetVertex.cloneWithoutGates();
			targetGroupVertex.setGroupMember(targetVertex);
		}
		QosGate inputGate = targetVertex.getInputGate(templateInputGate
				.getGateIndex());
		if (inputGate == null) {
			inputGate = templateInputGate.cloneWithoutEdgesAndVertex();
			targetVertex.setInputGate(inputGate);
		}

		QosEdge clonedEdge = templateEdge.cloneWithoutGates();
		clonedEdge.setOutputGate(outputGate);
		clonedEdge.setInputGate(inputGate);
	}

	// private static void createSubgraphGroupVertices(QosGraph qosGraph,
	// HashMap<JobVertexID, JobGraphSequence sequence) {
	//
	// HashMap<JobVertexID, QosGroupVertex> subgraphGroupVertices = new
	// HashMap<JobVertexID, QosGroupVertex>(
	// qosGraph.getNumberOfVertices());
	//
	// for (SequenceElement sequenceElem : sequence) {
	// if (sequenceElem.isVertex()) {
	//
	// ensureGroupVertexPresence(sequenceElem.getVertexID(),
	// subgraphGroupVertices, qosGraph);
	//
	// } else {
	//
	// QosGroupVertex source = ensureGroupVertexPresence(
	// sequenceElem.getSourceVertexID(),
	// subgraphGroupVertices, qosGraph);
	//
	// QosGroupVertex target = ensureGroupVertexPresence(
	// sequenceElem.getTargetVertexID(),
	// subgraphGroupVertices, qosGraph);
	//
	// QosGroupEdge templateEdge = qosGraph.getGroupVertexByID(
	// sequenceElem.getSourceVertexID()).getForwardEdge(
	// sequenceElem.getOutputGateIndex());
	// source.wireTo(target, templateEdge);
	//
	// }
	// }
	// }
	//
	// private static QosGroupVertex ensureGroupVertexPresence(JobVertexID
	// vertexID,
	// HashMap<JobVertexID, QosGroupVertex> subgraphGroupVertices,
	// QosGraph qosGraph) {
	//
	// QosGroupVertex toReturn = subgraphGroupVertices.get(vertexID);
	//
	// if (toReturn == null) {
	// QosGroupVertex template = qosGraph.getGroupVertexByID(vertexID);
	// toReturn = template.cloneWithoutMembersOrEdges();
	// subgraphGroupVertices.put(vertexID, toReturn);
	// }
	// return toReturn;
	// }
}
