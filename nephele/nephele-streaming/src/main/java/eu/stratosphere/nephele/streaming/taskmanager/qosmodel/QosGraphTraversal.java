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

import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;

/**
 * Provides a depth-first way of traversing the member elements (
 * {@link QosVertex} and {@link QosEdge}) of a QoS graph along a
 * JobGraphSequence.
 * 
 * Instances of this class are not thread-safe.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosGraphTraversal {

	private QosVertex startVertex;

	private HashSet<ExecutionVertexID> traversedVertices;

	private QosGraphTraversalListener listener;

	private boolean clearTraversedVerticesAfterTraversal;

	private JobGraphSequence sequence;

	private QosGraphTraversalCondition traversalCondition;

	public QosGraphTraversal(QosVertex startVertex, JobGraphSequence sequence,
			QosGraphTraversalListener listener) {
		this(startVertex, sequence, listener, null);
	}

	/**
	 * 
	 * Initializes QosGraphTraversal with the given start-vertex.
	 * 
	 * @param startVertex
	 *            Traversal starts from this vertex.
	 * @param sequence
	 *            The sequence to traverse the Qos graph along.
	 * @param listener
	 *            A callback that is invoked for each vertex or edge encountered
	 *            during depth first traversal.
	 * @param traversalCondition
	 *            A callback used to to determine whether to continue traversal
	 *            or not (only used during traverse...Conditional methods).
	 */
	public QosGraphTraversal(QosVertex startVertex, JobGraphSequence sequence,
			QosGraphTraversalListener listener,
			QosGraphTraversalCondition traversalCondition) {

		this.startVertex = startVertex;
		this.sequence = sequence;
		this.traversedVertices = new HashSet<ExecutionVertexID>();
		this.listener = listener;
		this.clearTraversedVerticesAfterTraversal = true;
		this.traversalCondition = traversalCondition;
	}

	/**
	 * When invoking the traversal methods with traverseOnce set to true, they
	 * track the already traversed vertices in a hash set. If you set invoke
	 * this method with false, the hash set will not be emptied after the
	 * invocation of a traversal method. This enables traverseOnce behavior
	 * across multiple invocations of the traversal methods. The default value
	 * is true.
	 * 
	 * @param cleartraversedVerticesAfterTraversal
	 */
	public void setClearTraversedVertices(
			boolean clearTraversedVerticesAfterTraversal) {
		this.clearTraversedVerticesAfterTraversal = clearTraversedVerticesAfterTraversal;
	}

	public void setStartVertex(QosVertex startVertex) {
		this.startVertex = startVertex;
	}

	public void setTraversalListener(QosGraphTraversalListener listener) {
		this.listener = listener;
	}

	public void setSequence(JobGraphSequence sequence) {
		this.sequence = sequence;
	}

	public void setTraversalCondition(
			QosGraphTraversalCondition traversalCondition) {
		this.traversalCondition = traversalCondition;
	}

	/**
	 * Equal to calling
	 * {@link #traverseGraphForwardAlongSequence(JobGraphSequence, true, true)}
	 * .
	 */
	public void traverseForward() {
		this.traverseForward(true, true);
	}

	/**
	 * Depth-first-traverses the QosGraph of the start-vertex, along the
	 * JobGraphSequence. Traversal starts at the start-vertex and for each
	 * encountered vertex or edge the given listener is called.
	 * 
	 * Corner case: Sequences may start/end with edges. If the start-vertex's
	 * group vertex is not part of the sequence, it must at least be the
	 * source/target of the first/last edge in the sequence. The listener will
	 * not be called for the start-vertex then.
	 * 
	 * @param includeStartVertex
	 *            Whether the listener should also be called for the
	 *            start-vertex. If its group vertex is not in the sequence, this
	 *            parameter has no effect.
	 * @param traverseOnce
	 *            Whether vertices of the QoS graph shall only be traversed once
	 *            at maximum. If set to true, traversed vertices will be
	 *            tracked, otherwise not. Also see
	 *            {@link #cleartraversedVerticesAfterTraversal()}).
	 */
	public void traverseForward(boolean includeStartVertex, boolean traverseOnce) {

		Deque<SequenceElement> afterDeque = this
				.getSequenceAfterIncludingStartVertex(this.sequence);

		if (afterDeque.isEmpty()) {
			return;
		}

		// do some sanity checking
		SequenceElement firstElem = afterDeque.getFirst();
		if (!QosGraphUtil.match(firstElem, this.startVertex)
				&& !QosGraphUtil.isEdgeAndStartsAtVertex(firstElem,
						this.startVertex)) {
			throw new RuntimeException(
					"If the start-vertex is not on the sequence it must at least be the source/target of the first/last edge in the sequence.");
		}

		if (!includeStartVertex
				&& QosGraphUtil.match(afterDeque.getFirst(), this.startVertex)) {
			afterDeque.removeFirst();
		}

		if (afterDeque.isEmpty()) {
			return;
		} else if (afterDeque.getFirst().isVertex()) {
			this.traverseForwardVertex(this.startVertex, afterDeque,
					traverseOnce);
		} else {
			QosGate outputGate = this.startVertex.getOutputGate(afterDeque
					.getFirst().getOutputGateIndex());
			for (QosEdge edge : outputGate.getEdges()) {
				this.traverseForwardEdge(edge, afterDeque, traverseOnce);
			}
		}

		if (this.clearTraversedVerticesAfterTraversal) {
			this.traversedVertices.clear();
		}
	}

	public void traverseForwardConditional() {
		Deque<SequenceElement> afterDeque = this
				.getSequenceAfterIncludingStartVertex(this.sequence);

		if (afterDeque.isEmpty()) {
			return;
		}

		// do some sanity checking
		SequenceElement firstElem = afterDeque.getFirst();
		if (!QosGraphUtil.match(firstElem, this.startVertex)
				&& !QosGraphUtil.isEdgeAndStartsAtVertex(firstElem,
						this.startVertex)) {
			throw new RuntimeException(
					"If the start-vertex is not on the sequence it must at least be the source/target of the first/last edge in the sequence.");
		}

		if (this.traversalCondition == null) {
			throw new RuntimeException("A traversal condition has to be set.");
		}

		if (afterDeque.isEmpty()) {
			return;
		} else if (afterDeque.getFirst().isVertex()) {
			this.traverseForwardConditionalVertex(this.startVertex, afterDeque);
		} else {
			QosGate outputGate = this.startVertex.getOutputGate(afterDeque
					.getFirst().getOutputGateIndex());

			for (QosEdge edge : outputGate.getEdges()) {
				this.traverseForwardConditionalEdge(edge, afterDeque);
			}
		}

		if (this.clearTraversedVerticesAfterTraversal) {
			this.traversedVertices.clear();
		}
	}

	private void traverseForwardConditionalEdge(QosEdge edge,
			Deque<SequenceElement> sequenceDeque) {

		if (!this.traversalCondition.shallTraverseEdge(edge,
				sequenceDeque.getFirst())) {
			return;
		}

		SequenceElement currentElem = sequenceDeque.removeFirst();

		this.listener.processQosEdge(edge, currentElem);

		if (!sequenceDeque.isEmpty()) {
			this.traverseForwardConditionalVertex(edge.getInputGate()
					.getVertex(), sequenceDeque);
		}

		sequenceDeque.addFirst(currentElem);
	}

	private void traverseForwardConditionalVertex(QosVertex vertex,
			Deque<SequenceElement> sequenceDeque) {

		if (!this.traversalCondition.shallTraverseVertex(vertex,
				sequenceDeque.getFirst())) {
			return;
		}

		SequenceElement currentElem = sequenceDeque.removeFirst();

		this.listener.processQosVertex(vertex, currentElem);

		if (!sequenceDeque.isEmpty()) {
			QosGate outputGate = vertex.getOutputGate(sequenceDeque.getFirst()
					.getOutputGateIndex());

			if (outputGate != null) {
				for (QosEdge edge : outputGate.getEdges()) {
					this.traverseForwardConditionalEdge(edge, sequenceDeque);
				}
			}
		}

		sequenceDeque.addFirst(currentElem);
	}

	private void traverseForwardVertex(QosVertex vertex,
			Deque<SequenceElement> sequenceDeque,
			boolean traverseOnce) {

		if (traverseOnce && this.traversedVertices.contains(vertex.getID())) {
			return;
		}

		SequenceElement currentElem = sequenceDeque.removeFirst();

		this.listener.processQosVertex(vertex, currentElem);

		if (traverseOnce) {
			this.traversedVertices.add(vertex.getID());
		}

		if (!sequenceDeque.isEmpty()) {
			QosGate outputGate = vertex.getOutputGate(sequenceDeque.getFirst()
					.getOutputGateIndex());
			for (QosEdge edge : outputGate.getEdges()) {
				this.traverseForwardEdge(edge, sequenceDeque, traverseOnce);
			}
		}

		sequenceDeque.addFirst(currentElem);
	}

	private void traverseForwardEdge(QosEdge edge,
			Deque<SequenceElement> sequenceDeque,
			boolean traverseOnce) {

		SequenceElement currentElem = sequenceDeque.removeFirst();

		this.listener.processQosEdge(edge, currentElem);

		if (!sequenceDeque.isEmpty()) {
			this.traverseForwardVertex(edge.getInputGate().getVertex(),
					sequenceDeque, traverseOnce);
		}

		sequenceDeque.addFirst(currentElem);
	}

	private LinkedList<SequenceElement> getSequenceAfterIncludingStartVertex(
			JobGraphSequence sequence) {

		boolean notInSequence = QosGraphUtil.isEdgeAndEndsAtVertex(
				sequence.getLast(), this.startVertex);
		if (notInSequence) {
			return new LinkedList<SequenceElement>();
		}

		LinkedList<SequenceElement> ret = new LinkedList<SequenceElement>(
				sequence);
		while (!ret.isEmpty()) {
			SequenceElement current = ret.getFirst();

			if (QosGraphUtil.match(current, this.startVertex)
					|| QosGraphUtil.isEdgeAndStartsAtVertex(current,
							this.startVertex)) {
				break;
			}

			ret.removeFirst();
		}

		return ret;
	}

	/**
	 * Equal to calling {@link #traverseGraphBackwardAlongSequence(true, true)}
	 * .
	 */
	public void traverseBackward() {

		this.traverseBackward(true, true);
	}

	/**
	 * Depth-first-traverses the QosGraph of the start-vertex in backward
	 * direction, along the given JobGraphSequence. Traversal starts at the
	 * start-vertex and for each encountered vertex or edge the given listener
	 * is called.
	 * 
	 * Corner case: Sequences may start/end with edges. If the start-vertex's
	 * group vertex is not part of the sequence, it must at least be the
	 * source/target of the first/last edge in the sequence. The listener will
	 * not be called for the start-vertex then.
	 * 
	 * @param sequence
	 *            Determines which path to walk backwards.
	 * 
	 * @param includeStartVertex
	 *            Whether the listener should also be called for the
	 *            start-vertex. If its group vertex is not in the sequence, this
	 *            parameter has no effect.
	 * @param traverseOnce
	 *            Whether vertices of the QoS graph shall only be traversed once
	 *            at maximum. If set to true, traversed vertices will be
	 *            tracked, otherwise not. Also see
	 *            {@link #cleartraversedVerticesAfterTraversal()}).
	 */
	public void traverseBackward(boolean includeStartVertex,
			boolean traverseOnce) {

		LinkedList<SequenceElement> elemsBefore = this
				.getSequenceBeforeIncludingStartVertex(this.sequence);

		if (elemsBefore.isEmpty()) {
			return;
		}

		// do some sanity checking
		SequenceElement lastElem = elemsBefore.getLast();
		if (!QosGraphUtil.match(lastElem, this.startVertex)
				&& !QosGraphUtil.isEdgeAndEndsAtVertex(lastElem,
						this.startVertex)) {
			throw new RuntimeException(
					"If the start-vertex is not on the sequence it must at least be the source/target of the first/last edge in the sequence.");
		}

		if (!includeStartVertex
				&& QosGraphUtil.match(elemsBefore.getLast(), this.startVertex)) {
			elemsBefore.removeLast();
		}

		if (elemsBefore.isEmpty()) {
			return;
		} else if (elemsBefore.getLast().isVertex()) {
			this.traverseVertexBackward(this.startVertex, elemsBefore,
					traverseOnce);
		} else {
			QosGate inputGate = this.startVertex.getInputGate(elemsBefore
					.getLast().getInputGateIndex());
			for (QosEdge edge : inputGate.getEdges()) {
				this.traverseEdgeBackward(edge, elemsBefore, traverseOnce);
			}
		}

		if (this.clearTraversedVerticesAfterTraversal) {
			this.traversedVertices.clear();
		}
	}

	private LinkedList<SequenceElement> getSequenceBeforeIncludingStartVertex(
			JobGraphSequence sequence) {

		boolean notInSequence = QosGraphUtil.isEdgeAndStartsAtVertex(
				sequence.getFirst(), this.startVertex);
		if (notInSequence) {
			return new LinkedList<SequenceElement>();
		}

		LinkedList<SequenceElement> ret = new LinkedList<SequenceElement>(
				sequence);

		while (!ret.isEmpty()) {
			SequenceElement current = ret.getLast();

			if (QosGraphUtil.match(current, this.startVertex)
					|| QosGraphUtil.isEdgeAndEndsAtVertex(current,
							this.startVertex)) {
				break;
			}

			ret.removeLast();
		}

		return ret;
	}

	private void traverseVertexBackward(QosVertex vertex,
			Deque<SequenceElement> sequenceDeque,
			boolean traverseOnce) {

		if (traverseOnce && this.traversedVertices.contains(vertex.getID())) {
			return;
		}

		SequenceElement currentElem = sequenceDeque.removeLast();

		this.listener.processQosVertex(vertex, currentElem);

		if (traverseOnce) {
			this.traversedVertices.add(vertex.getID());
		}

		if (!sequenceDeque.isEmpty()) {
			QosGate inputGate = vertex.getInputGate(sequenceDeque.getLast()
					.getInputGateIndex());
			for (QosEdge edge : inputGate.getEdges()) {
				this.traverseEdgeBackward(edge, sequenceDeque, traverseOnce);
			}
		}

		sequenceDeque.addLast(currentElem);
	}

	private void traverseEdgeBackward(QosEdge edge,
			Deque<SequenceElement> sequenceDeque,
			boolean traverseOnce) {

		SequenceElement currentElem = sequenceDeque.removeLast();

		this.listener.processQosEdge(edge, currentElem);

		if (!sequenceDeque.isEmpty()) {
			this.traverseVertexBackward(edge.getOutputGate().getVertex(),
					sequenceDeque, traverseOnce);
		}

		sequenceDeque.addLast(currentElem);
	}
}
