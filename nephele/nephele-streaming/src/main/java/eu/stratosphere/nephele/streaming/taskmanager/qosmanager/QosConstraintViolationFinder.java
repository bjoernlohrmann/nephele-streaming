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
package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.util.ArrayList;
import java.util.Collections;

import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeQosData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphMember;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversal;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversalCondition;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversalListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.VertexQosData;

/**
 * Instances of this class can be used by a Qos manager to look for violations
 * of a Qos constraint inside a Qos graph. Sequences of Qos vertices and edges
 * that violate the Qos constraint are handed to a
 * {@link QosConstraintViolationListener}.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosConstraintViolationFinder implements QosGraphTraversalListener,
		QosGraphTraversalCondition {

	private QosGraph qosGraph;

	private QosGraphTraversal graphTraversal;

	private QosSequenceLatencySummary sequenceSummary;

	private int sequenceLength;

	private JobGraphLatencyConstraint constraint;

	private ArrayList<QosGraphMember> currentSequenceMembers;

	private QosConstraintViolationListener constraintViolationListener;

	private QosConstraintViolationReport violationReport;
	
	private long inactivityThresholdTime;

	public QosConstraintViolationFinder(LatencyConstraintID constraintID,
			QosGraph qosGraph,
			QosConstraintViolationListener constraintViolationListener,
			long inactivityThresholdTime) {

		this.qosGraph = qosGraph;
		this.constraint = qosGraph.getConstraintByID(constraintID);
		this.violationReport = new QosConstraintViolationReport(this.constraint);
		this.constraintViolationListener = constraintViolationListener;
		this.inactivityThresholdTime = inactivityThresholdTime;

		this.graphTraversal = new QosGraphTraversal(null,
				this.constraint.getSequence(), this, this);
		this.sequenceSummary = new QosSequenceLatencySummary(this.constraint.getSequence());
		this.sequenceLength = this.constraint.getSequence().size();

		// init sequence with nulls so that during graph traversal we can
		// just invoke set(index, member).
		this.currentSequenceMembers = new ArrayList<QosGraphMember>(
				this.sequenceLength);
		Collections.addAll(this.currentSequenceMembers,
				new QosGraphMember[this.sequenceLength]);
	}

	public QosConstraintViolationReport scanSequencesForQosConstraintViolations() {

		JobGraphSequence sequence = this.constraint.getSequence();
		QosGroupVertex startGroupVertex;
		if (sequence.getFirst().isVertex()) {
			startGroupVertex = this.qosGraph.getGroupVertexByID(sequence
					.getFirst().getVertexID());
		} else {
			startGroupVertex = this.qosGraph.getGroupVertexByID(sequence
					.getFirst().getSourceVertexID());
		}

		for (QosVertex startMemberVertex : startGroupVertex.getMembers()) {
			this.graphTraversal.setStartVertex(startMemberVertex);
			this.graphTraversal.traverseForwardConditional();
		}
		
		return this.violationReport;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.streaming.taskmanager.qosmodel.
	 * QosGraphTraversalCondition
	 * #shallTraverseEdge(eu.stratosphere.nephele.streaming
	 * .taskmanager.qosmodel.QosEdge,
	 * eu.stratosphere.nephele.streaming.SequenceElement)
	 */
	@Override
	public boolean shallTraverseEdge(QosEdge edge,
			SequenceElement seqElem) {

		boolean isActive = true;

		if (seqElem.getIndexInSequence() == 0) {
			int outputGateIndex = edge.getOutputGate().getGateIndex();
			VertexQosData sourceVertexQosData = edge.getOutputGate()
					.getVertex().getQosData();
			sourceVertexQosData.dropOlderData(-1, outputGateIndex,
					inactivityThresholdTime);
			isActive = isActive
					&& sourceVertexQosData.hasNewerData(-1, outputGateIndex,
							inactivityThresholdTime);
		}

		if (seqElem.getIndexInSequence() == sequenceLength - 1) {
			int inputGateIndex = edge.getInputGate().getGateIndex();
			VertexQosData targetVertexQosData = edge.getInputGate().getVertex()
					.getQosData();
			targetVertexQosData.dropOlderData(inputGateIndex, -1,
					inactivityThresholdTime);
			isActive = isActive
					&& targetVertexQosData.hasNewerData(inputGateIndex, -1,
							inactivityThresholdTime);
		}

		EdgeQosData edgeQos = edge.getQosData();
		edgeQos.dropOlderData(inactivityThresholdTime);
		isActive = isActive && edgeQos.hasNewerData(inactivityThresholdTime);

		return isActive;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.streaming.taskmanager.qosmodel.
	 * QosGraphTraversalCondition
	 * #shallTraverseVertex(eu.stratosphere.nephele.streaming
	 * .taskmanager.qosmodel.QosVertex,
	 * eu.stratosphere.nephele.streaming.SequenceElement)
	 */
	@Override
	public boolean shallTraverseVertex(QosVertex vertex,
			SequenceElement seqElem) {

		VertexQosData qosData = vertex.getQosData();

		qosData.dropOlderData(seqElem.getInputGateIndex(),
				seqElem.getOutputGateIndex(), inactivityThresholdTime);

		return qosData.hasNewerData(seqElem.getInputGateIndex(),
				seqElem.getOutputGateIndex(), inactivityThresholdTime);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.streaming.taskmanager.qosmodel.
	 * QosGraphTraversalListener
	 * #processQosVertex(eu.stratosphere.nephele.streaming
	 * .taskmanager.qosmodel.QosVertex,
	 * eu.stratosphere.nephele.streaming.SequenceElement)
	 */
	@Override
	public void processQosVertex(QosVertex vertex,
			SequenceElement sequenceElem) {

		int index = sequenceElem.getIndexInSequence();
		this.currentSequenceMembers.set(index, vertex);

		if (index + 1 == this.sequenceLength) {
			this.handleFullSequence();
		}
	}

	private void handleFullSequence() {
		sequenceSummary.update(this.currentSequenceMembers);
		
		violationReport.addQosSequenceLatencySummary(sequenceSummary);

		double constraintViolatedByMillis = this.sequenceSummary.getSequenceLatency()
				- this.constraint.getLatencyConstraintInMillis();

		// only act on violations of >5% of the constraint
		if (Math.abs(constraintViolatedByMillis)
				/ this.constraint.getLatencyConstraintInMillis() > 0.05) {
			this.constraintViolationListener.handleViolatedConstraint(this.constraint,
					this.currentSequenceMembers, 
					this.sequenceSummary);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.streaming.taskmanager.qosmodel.
	 * QosGraphTraversalListener
	 * #processQosEdge(eu.stratosphere.nephele.streaming
	 * .taskmanager.qosmodel.QosEdge,
	 * eu.stratosphere.nephele.streaming.SequenceElement)
	 */
	@Override
	public void processQosEdge(QosEdge edge,
			SequenceElement sequenceElem) {

		int index = sequenceElem.getIndexInSequence();
		this.currentSequenceMembers.set(index, edge);

		if (index + 1 == this.sequenceLength) {
			this.handleFullSequence();
		}
	}
}
