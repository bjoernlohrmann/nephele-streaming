package eu.stratosphere.nephele.streaming.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.CpuLoadClassifier.CpuLoad;

/**
 * Used to signal by a task manager to the job manager, that the CPU load state
 * of a task has changed.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class TaskCpuLoadChange extends AbstractSerializableQosMessage {

	private ExecutionVertexID vertexId;

	private double cpuUtilization;

	public TaskCpuLoadChange() {
	}
	
	public TaskCpuLoadChange(JobID jobID, ExecutionVertexID vertexId, double cpuUtilization) {

		super(jobID);
		this.vertexId = vertexId;
		this.cpuUtilization = cpuUtilization;
	}

	public CpuLoad getLoadState() {
		return CpuLoadClassifier.fromCpuUtilization(cpuUtilization);
	}

	public ExecutionVertexID getVertexId() {
		return vertexId;
	}

	public double getCpuUtilization() {
		return cpuUtilization;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		this.vertexId.write(out);
		out.writeDouble(cpuUtilization);
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		vertexId = new ExecutionVertexID();
		vertexId.read(in);
		cpuUtilization = in.readDouble();
	}
}
