package eu.stratosphere.nephele.streaming.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Used to signal by a task manager to the job manager, that the CPU load state
 * of a task has changed.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class TaskLoadStateChange extends AbstractSerializableQosMessage {

	public enum LoadState {
		LOW, MEDIUM, HIGH
	}

	private LoadState loadState;

	private ExecutionVertexID vertexId;

	private double cpuUtilization;

	public TaskLoadStateChange() {
	}
	
	public TaskLoadStateChange(JobID jobID, LoadState loadState,
			ExecutionVertexID vertexId, double cpuUtilization) {

		super(jobID);
		this.loadState = loadState;
		this.vertexId = vertexId;
		this.cpuUtilization = cpuUtilization;
	}

	public LoadState getLoadState() {
		return loadState;
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
		out.writeUTF(loadState.name());
		out.writeDouble(cpuUtilization);
	}

	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		vertexId = new ExecutionVertexID();
		vertexId.read(in);
		loadState = LoadState.valueOf(in.readUTF());
		cpuUtilization = in.readDouble();
	}
}
