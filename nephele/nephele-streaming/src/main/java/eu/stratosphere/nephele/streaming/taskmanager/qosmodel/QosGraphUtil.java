/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import eu.stratosphere.nephele.streaming.SequenceElement;

/**
 * Utility class to work with Qos graphs.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosGraphUtil {

	public static boolean match(SequenceElement sequenceElement,
			QosVertex vertex) {
		return match(sequenceElement, vertex.getGroupVertex());
	}

	public static boolean match(SequenceElement sequenceElement,
			QosGroupVertex groupVertex) {
		return sequenceElement.isVertex()
				&& sequenceElement.getVertexID().equals(
						groupVertex.getJobVertexID());
	}

	public static boolean match(SequenceElement sequenceElement,
			QosGroupEdge groupEdge) {

		return sequenceElement.isEdge()
				&& sequenceElement.getSourceVertexID().equals(
						groupEdge.getSourceVertex().getJobVertexID())
				&& sequenceElement.getTargetVertexID().equals(
						groupEdge.getTargetVertex().getJobVertexID())
				&& sequenceElement.getOutputGateIndex() == groupEdge
						.getOutputGateIndex()
				&& sequenceElement.getInputGateIndex() == groupEdge
						.getInputGateIndex();
	}

	public static boolean match(SequenceElement sequenceElement,
			QosEdge edge) {

		return sequenceElement.isEdge()
				&& sequenceElement.getSourceVertexID().equals(
						edge.getOutputGate().getVertex().getGroupVertex()
								.getJobVertexID())
				&& sequenceElement.getTargetVertexID().equals(
						edge.getInputGate().getVertex().getGroupVertex()
								.getJobVertexID())
				&& sequenceElement.getOutputGateIndex() == edge.getOutputGate()
						.getGateIndex()
				&& sequenceElement.getInputGateIndex() == edge.getInputGate()
						.getGateIndex();
	}

	public static boolean isEdgeAndEndsAtVertex(
			SequenceElement edge, QosGroupVertex groupVertex) {

		return edge.isEdge()
				&& edge.getTargetVertexID()
						.equals(groupVertex.getJobVertexID());
	}

	public static boolean isEdgeAndEndsAtVertex(
			SequenceElement edge, QosVertex vertex) {

		return isEdgeAndEndsAtVertex(edge, vertex.getGroupVertex());
	}

	public static boolean isEdgeAndStartsAtVertex(
			SequenceElement edge, QosVertex vertex) {

		return isEdgeAndStartsAtVertex(edge, vertex.getGroupVertex());
	}

	public static boolean isEdgeAndStartsAtVertex(
			SequenceElement edge, QosGroupVertex groupVertex) {

		return edge.isEdge()
				&& edge.getSourceVertexID()
						.equals(groupVertex.getJobVertexID());
	}

}
