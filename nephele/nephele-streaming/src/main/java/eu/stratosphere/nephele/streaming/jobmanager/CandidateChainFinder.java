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
package eu.stratosphere.nephele.streaming.jobmanager;

import java.util.LinkedList;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGate;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversal;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphTraversalListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class CandidateChainFinder implements QosGraphTraversalListener {

	public InstanceConnectionInfo currentChainTaskManager;

	public LinkedList<ExecutionVertexID> currentChain;

	public int currentChainLastElementSequenceIndex;

	private QosGraphTraversal traversal;

	private CandidateChainListener chainListener;

	private ExecutionGraph executionGraph;

	public CandidateChainFinder(CandidateChainListener chainListener,
			ExecutionGraph executionGraph) {

		this.executionGraph = executionGraph;
		this.chainListener = chainListener;
		this.currentChain = new LinkedList<ExecutionVertexID>();
		this.currentChainLastElementSequenceIndex = -1;
		this.currentChainTaskManager = null;

		this.traversal = new QosGraphTraversal(null, null, this);
		this.traversal.setClearTraversedVertices(false);
	}

	@Override
	public void processQosVertex(QosVertex vertex,
			SequenceElement sequenceElem) {

		if (this.currentChain.isEmpty()) {
			tryToStartChain(vertex, sequenceElem);
		} else {
			appendToOrRestartChain(vertex, sequenceElem);
		}
	}

	private void appendToOrRestartChain(QosVertex vertex,
			SequenceElement sequenceElem) {

		int noOfInputGatesInExecutionGraph = this.executionGraph
				.getVertexByID(vertex.getID()).getNumberOfInputGates();

		QosGate inputGate = vertex.getInputGate(sequenceElem
				.getInputGateIndex());

		if (inputGate != null
				&& noOfInputGatesInExecutionGraph == 1
				&& inputGate.getNumberOfEdges() == 1 // noOfChannelsOnInputGate
				&& vertex.getExecutingInstance().equals(
						this.currentChainTaskManager)
				&& this.currentChainLastElementSequenceIndex < sequenceElem
						.getIndexInSequence()) {

			this.currentChain.add(vertex.getID());
			this.currentChainLastElementSequenceIndex = sequenceElem
					.getIndexInSequence();
		} else {
			finishCurrentChain();
			tryToStartChain(vertex, sequenceElem);
		}
	}

	private void tryToStartChain(QosVertex vertex,
			SequenceElement sequenceElem) {

		QosGate outputGate = vertex.getOutputGate(sequenceElem
				.getOutputGateIndex());

		if (outputGate == null) {
			return;
		}

		int noOfEdgesInOutputGate = outputGate.getNumberOfEdges();

		int noOfOutputGatesInExecutionGraph = this.executionGraph
				.getVertexByID(vertex.getID()).getNumberOfOutputGates();

		if (noOfEdgesInOutputGate == 1 && noOfOutputGatesInExecutionGraph == 1) {
			this.currentChainTaskManager = vertex.getExecutingInstance();
			this.currentChain.add(outputGate.getVertex().getID());
			this.currentChainLastElementSequenceIndex = sequenceElem
					.getIndexInSequence();
		}
	}

	@Override
	public void processQosEdge(QosEdge edge,
			SequenceElement sequenceElem) {
		// do nothing
	}

	private void finishCurrentChain() {
		if (this.currentChain.size() >= 2) {
			this.chainListener.handleCandidateChain(this.currentChainTaskManager,
					this.currentChain);
			this.currentChain = new LinkedList<ExecutionVertexID>();
		} else {
			this.currentChain.clear();
		}

		this.currentChainTaskManager = null;
		this.currentChainLastElementSequenceIndex = -1;

	}

	public void findChainsAlongConstraint(LatencyConstraintID constraintID,
			QosGraph qosGraph) {

		this.traversal.setSequence(qosGraph.getConstraintByID(constraintID)
				.getSequence());

		for (QosGroupVertex groupStartVertex : qosGraph.getStartVertices()) {
			for (QosVertex startVertex : groupStartVertex.getMembers()) {
				this.traversal.setStartVertex(startVertex);
				this.traversal.traverseForward();
				this.finishCurrentChain();
			}
		}
	}
}
