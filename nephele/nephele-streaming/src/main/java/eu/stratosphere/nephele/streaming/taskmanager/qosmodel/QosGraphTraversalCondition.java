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
 * Interface to implement traversal conditions for {@link QosGraphTraversal}.
 * {@link QosGraphTraversal#traverseForwardConditional()} uses an instance of
 * this class to check whether depth-first graph traversal should continue after
 * a certain vertex/edge has been reached.
 * 
 * @author Bjoern Lohrmann
 */
public interface QosGraphTraversalCondition {

	/**
	 * A callback used during Qos graph traversal, to check whether traversal
	 * shall continue with the given edge.
	 * 
	 * @return True if traversal shall continue with the edge, false if
	 *         traversal should turn around at the given edge.
	 */
	public boolean shallTraverseEdge(QosEdge edge,
			SequenceElement sequenceElement);

	/**
	 * A callback used during Qos graph traversal, to check whether traversal
	 * shall continue with the given vertex.
	 * 
	 * @return True if traversal shall continue with the vertex, false if
	 *         traversal should turn around at the given vertex.
	 */
	public boolean shallTraverseVertex(QosVertex vertex,
			SequenceElement sequenceElement);

}
