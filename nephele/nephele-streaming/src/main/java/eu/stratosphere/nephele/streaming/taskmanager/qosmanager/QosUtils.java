package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

/**
 * Utility methods for use by Qos managers.
 * 
 * @author Bjoern Lohrmann
 */
public class QosUtils {

	public static String formatName(ManagementAttachment managementAttachment) {
		if (managementAttachment instanceof ManagementVertex) {
			return formatName((ManagementVertex) managementAttachment);
		}
		return formatName((ManagementEdge) managementAttachment);
	}

	public static String formatName(ManagementEdge edge) {
		return formatName(edge.getSource().getVertex()) + "->"
				+ formatName(edge.getTarget().getVertex());
	}

	public static String formatName(ManagementVertex vertex) {
		String name = vertex.getName();
		for (int i = 0; i < vertex.getGroupVertex().getNumberOfGroupMembers(); i++) {
			if (vertex.getGroupVertex().getGroupMember(i) == vertex) {
				name += i;
				break;
			}
		}
		return name;
	}

	public static String formatName(ExecutionVertex vertex) {
		String name = vertex.getName();
		for (int i = 0; i < vertex.getGroupVertex()
				.getCurrentNumberOfGroupMembers(); i++) {
			if (vertex.getGroupVertex().getGroupMember(i) == vertex) {
				name += i;
				break;
			}
		}
		return name;
	}

	public static long alignToInterval(long timestampInMillis, long interval) {
		long remainder = timestampInMillis % interval;

		return timestampInMillis - remainder;
	}

	// FIXME remove this code
	// /**
	// * @return Returns true when the group edge of the given (member-level)
	// * output gate is the last element of a constraint in the given Qos
	// * graph.
	// */
	// public static boolean isOnLastElementOfConstraint(QosGate outputGate,
	// QosGraph qosGraph) {
	//
	// if (!outputGate.isOutputGate()) {
	// throw new RuntimeException(
	// "You must only provide output gates to this method. This is a bug.");
	// }
	//
	// JobVertexID sourceGroupVertexID = outputGate.getVertex()
	// .getGroupVertex().getJobVertexID();
	//
	// JobVertexID targetGroupVertexID = outputGate.getVertex()
	// .getGroupVertex().getForwardEdge(outputGate.getGateIndex())
	// .getTargetVertex().getJobVertexID();
	//
	// for (JobGraphLatencyConstraint constraint : qosGraph.getConstraints()) {
	// SequenceElement lastSequenceElement = constraint
	// .getSequence().getLast();
	// if (lastSequenceElement.isEdge()
	// && lastSequenceElement.getSourceVertexID().equals(
	// sourceGroupVertexID)
	// && lastSequenceElement.getTargetVertexID().equals(
	// targetGroupVertexID)) {
	// return true;
	// }
	// }
	//
	// return false;
	// }
	//
	// /**
	// * @return Returns true when the group edge of the given (member-level)
	// * input gate is the first element of a constraint in the given Qos
	// * graph.
	// */
	// public static boolean isOnFirstElementOfConstraint(QosGate inputGate,
	// QosGraph qosGraph) {
	//
	// if (!inputGate.isInputGate()) {
	// throw new RuntimeException(
	// "You must only provide input gates to this method. This is a bug.");
	// }
	//
	// JobVertexID targetGroupVertexID = inputGate.getVertex()
	// .getGroupVertex().getJobVertexID();
	//
	// JobVertexID sourceGroupVertexID = inputGate.getVertex()
	// .getGroupVertex().getBackwardEdge(inputGate.getGateIndex())
	// .getSourceVertex().getJobVertexID();
	//
	// for (JobGraphLatencyConstraint constraint : qosGraph.getConstraints()) {
	// SequenceElement firstSequenceElement = constraint
	// .getSequence().getFirst();
	// if (firstSequenceElement.isEdge()
	// && firstSequenceElement.getSourceVertexID().equals(
	// sourceGroupVertexID)
	// && firstSequenceElement.getTargetVertexID().equals(
	// targetGroupVertexID)) {
	// return true;
	// }
	// }
	//
	// return false;
	// }

}
