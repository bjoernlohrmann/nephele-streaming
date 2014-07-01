/**
 * 
 */
package eu.stratosphere.nephele.taskmanager;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;

public class TaskSuspendResult extends AbstractTaskResult {

	public TaskSuspendResult(final ExecutionVertexID vertexID, final ReturnCode returnCode) {
		super(vertexID, returnCode);
	}

	public TaskSuspendResult() {
		super();
	}
}
