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

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphMember;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversal;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversalCondition;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversalListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

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

	private SequenceQosSummary sequenceSummary;

	private int sequenceLength;

	private JobGraphLatencyConstraint constraint;

	private ArrayList<QosGraphMember> currentSequenceMembers;

	private QosConstraintViolationListener constraintViolationListener;

	private QosConstraintSummary constraintSummary;

	public QosConstraintViolationFinder(LatencyConstraintID constraintID,
			QosGraph qosGraph,
			QosConstraintViolationListener constraintViolationListener) {

		this(constraintID, qosGraph, constraintViolationListener, null);
	}

	public QosConstraintViolationFinder(LatencyConstraintID constraintID,
			QosGraph qosGraph,
			QosConstraintViolationListener constraintViolationListener,
			QosLogger logger) {

		this.qosGraph = qosGraph;
		this.constraint = qosGraph.getConstraintByID(constraintID);
		this.constraintSummary = new QosConstraintSummary(this.constraint);
		this.constraintViolationListener = constraintViolationListener;

		this.graphTraversal = new QosGraphTraversal(null,
				this.constraint.getSequence(), this, this);
		this.sequenceSummary = new SequenceQosSummary(this.constraint.getSequence());
		this.sequenceLength = this.constraint.getSequence().size();

		// init sequence with nulls so that during graph traversal we can
		// just invoke set(index, member).
		this.currentSequenceMembers = new ArrayList<QosGraphMember>(
				this.sequenceLength);
		Collections.addAll(this.currentSequenceMembers,
				new QosGraphMember[this.sequenceLength]);

	}

	public QosConstraintSummary findSequencesWithViolatedQosConstraint() {

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
		
		return this.constraintSummary;
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
			SequenceElement<JobVertexID> sequenceElement) {

		return edge.getQosData().isActive();
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
			SequenceElement<JobVertexID> sequenceElement) {
		return vertex.getQosData().isActive(
				sequenceElement.getInputGateIndex(),
				sequenceElement.getOutputGateIndex());
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
			SequenceElement<JobVertexID> sequenceElem) {

		int index = sequenceElem.getIndexInSequence();
		this.currentSequenceMembers.set(index, vertex);

		if (index + 1 == this.sequenceLength) {
			this.handleFullSequence();
		}
	}

	private void handleFullSequence() {
		sequenceSummary.update(this.currentSequenceMembers);
		
		constraintSummary.addMemberSequenceSummary(sequenceSummary);

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
			SequenceElement<JobVertexID> sequenceElem) {

		int index = sequenceElem.getIndexInSequence();
		this.currentSequenceMembers.set(index, edge);

		if (index + 1 == this.sequenceLength) {
			this.handleFullSequence();
		}
	}
}
