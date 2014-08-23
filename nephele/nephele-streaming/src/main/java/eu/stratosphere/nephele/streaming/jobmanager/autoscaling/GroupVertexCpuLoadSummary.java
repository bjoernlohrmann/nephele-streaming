package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.Map;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.streaming.message.CpuLoadClassifier;
import eu.stratosphere.nephele.streaming.message.CpuLoadClassifier.CpuLoad;
import eu.stratosphere.nephele.streaming.message.TaskCpuLoadChange;

public class GroupVertexCpuLoadSummary {

	private int lows = 0;

	private int mediums = 0;

	private int highs = 0;

	private int unknowns = 0;

	private double averageCpuUtilization = 0;

	public GroupVertexCpuLoadSummary(
			Map<ExecutionVertexID, TaskCpuLoadChange> taskCpuLoads,
			ExecutionGroupVertex groupVertex)
			throws UnexpectedVertexExecutionStateException {

		int noOfRunningTasks = groupVertex
				.getCurrentElasticNumberOfRunningSubtasks();

		for (int i = 0; i < noOfRunningTasks; i++) {
			ExecutionVertex execVertex = groupVertex.getGroupMember(i);
			ExecutionState vertexState = execVertex.getExecutionState();

			switch (vertexState) {
			case RUNNING:
				if (taskCpuLoads.containsKey(execVertex.getID())) {
					TaskCpuLoadChange taskLoad = taskCpuLoads.get(execVertex
							.getID());

					averageCpuUtilization += taskLoad.getCpuUtilization();

					switch (taskLoad.getLoadState()) {
					case LOW:
						lows++;
						break;
					case MEDIUM:
						mediums++;
						break;
					case HIGH:
						highs++;
						break;
					}
				} else {
					unknowns++;
				}
				break;
			case SUSPENDING:
			case SUSPENDED:
				break;
			default:
				throw new UnexpectedVertexExecutionStateException();
			}
		}

		int aggregatedTasks = lows + mediums + highs;

		if (aggregatedTasks == 0) {
			throw new RuntimeException(
					"No running tasks with available CPU utilization data");
		} else {
			averageCpuUtilization /= aggregatedTasks;
		}
	}

	public int getLows() {
		return lows;
	}

	public int getMediums() {
		return mediums;
	}

	public int getHighs() {
		return highs;
	}

	public int getUnknowns() {
		return unknowns;
	}

	public double getAvgCpuUtilization() {
		return averageCpuUtilization;
	}
	
	public CpuLoad getAvgCpuLoad() {
		return CpuLoadClassifier.fromCpuUtilization(averageCpuUtilization);
	}
}
