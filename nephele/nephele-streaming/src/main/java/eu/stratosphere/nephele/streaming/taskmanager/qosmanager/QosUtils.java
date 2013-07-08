package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

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
}
