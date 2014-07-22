package eu.stratosphere.nephele.streaming.jobmanager;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mockito.Matchers;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.streaming.ConstraintUtil;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFactory;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;

/**
 * TODO refactoring when issue-1999 is merged with master to avoid code
 * duplication
 *
 * To use this class you'll have to annotate your test class in the
 * following way:
 *
 * @PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
 *                   AllocatedResource.class })
 * @RunWith(PowerMockRunner.class)
 *
 * @author Bernd Louis
 */
public class QosGraphFixtureMultiInstanceConnections {

	/**
	 * Looks like this I(2) ->(P) -> T(2) ->(P) -> O(2)
	 */
	public JobGraph jobGraph;
	public JobInputVertex jobInputVertex;
	public JobTaskVertex jobTaskVertex;
	public JobOutputVertex jobOutputVertex;

	public ExecutionGraph executionGraph;
	public ExecutionGroupVertex execInputVertex;
	public ExecutionGroupVertex execTaskVertex;
	public ExecutionGroupVertex execOutputVertex;
	public InstanceConnectionInfo[][] instanceConnectionInfos;

	public JobGraphLatencyConstraint constraintFull;
	public HashMap<LatencyConstraintID, QosGraph> constraints;

	/**
	 * Looks like this I(1) ->(B) T(2) ->(B) O(1)
	 */
	public JobGraph jobGraph2;
	public JobInputVertex jobInputVertex2;
	public JobTaskVertex jobTaskVertex2;
	public JobOutputVertex jobOutputVertex2;

	public ExecutionGraph executionGraph2;
	public ExecutionGroupVertex execInputVertex2;
	public ExecutionGroupVertex execTaskVertex2;
	public ExecutionGroupVertex execOutputVertex2;
	public InstanceConnectionInfo[][] instanceConnectionInfos2;
	public HashMap<LatencyConstraintID, QosGraph> constraints2;

	public QosGraphFixtureMultiInstanceConnections()
			throws JobGraphDefinitionException, GraphConversionException,
			IOException {
		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			BasicConfigurator.configure();
			Logger.getRootLogger().setLevel(Level.WARN);
		}

		this.makeJobGraph();
		this.makeExecutionGraph();
		this.makeConstraints();
	}

	private void makeJobGraph() throws JobGraphDefinitionException {
		this.jobGraph = new JobGraph();

		this.jobInputVertex = new JobInputVertex("vInput", this.jobGraph);
		this.jobInputVertex.setInputClass(DummyInputTask.class);
		this.jobInputVertex.setNumberOfSubtasks(2);

		this.jobTaskVertex = new JobTaskVertex("vTask", this.jobGraph);
		this.jobTaskVertex.setTaskClass(DummyTask.class);
		this.jobTaskVertex.setNumberOfSubtasks(2);

		this.jobOutputVertex = new JobOutputVertex("vOutput", this.jobGraph);
		this.jobOutputVertex.setOutputClass(DummyOutputTask.class);
		this.jobOutputVertex.setNumberOfSubtasks(2);

		this.jobInputVertex.connectTo(this.jobTaskVertex, ChannelType.NETWORK,
				DistributionPattern.POINTWISE);
		this.jobTaskVertex.connectTo(this.jobOutputVertex, ChannelType.NETWORK,
				DistributionPattern.POINTWISE);

		this.jobGraph2 = new JobGraph();
		this.jobInputVertex2 = new JobInputVertex("vInput2", this.jobGraph2);
		this.jobInputVertex2.setInputClass(DummyInputTask.class);
		this.jobInputVertex2.setNumberOfSubtasks(1);

		this.jobTaskVertex2 = new JobTaskVertex("vTask2", this.jobGraph2);
		this.jobTaskVertex2.setTaskClass(DummyTask.class);
		this.jobTaskVertex2.setNumberOfSubtasks(2);

		this.jobOutputVertex2 = new JobOutputVertex("vOutput2", this.jobGraph2);
		this.jobOutputVertex2.setOutputClass(DummyOutputTask.class);
		this.jobOutputVertex2.setNumberOfSubtasks(1);

		// connect

		this.jobInputVertex2.connectTo(this.jobTaskVertex2,
				ChannelType.NETWORK,
				DistributionPattern.BIPARTITE);
		this.jobTaskVertex2.connectTo(this.jobOutputVertex2,
				ChannelType.NETWORK,
				DistributionPattern.BIPARTITE);
	}

	/**
	 * TODO refactor using Util methods once they're merged
	 * 
	 * @throws GraphConversionException
	 */
	private void makeExecutionGraph() throws GraphConversionException {
		mockStatic(ExecutionSignature.class);

		ExecutionSignature execSig = mock(ExecutionSignature.class);

		when(
				ExecutionSignature.createSignature(
						Matchers.eq(AbstractGenericInputTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);
		when(
				ExecutionSignature.createSignature(
						Matchers.eq(AbstractTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);
		when(
				ExecutionSignature.createSignature(
						Matchers.eq(AbstractOutputTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);

		InstanceManager mockInstanceManager = mock(InstanceManager.class);
		InstanceType instType = new InstanceType();
		when(mockInstanceManager.getDefaultInstanceType()).thenReturn(instType);

		this.executionGraph = new ExecutionGraph(this.jobGraph,
				mockInstanceManager);

		ExecutionStage stage = this.executionGraph.getStage(0);
		for (int i = 0; i < stage.getNumberOfStageMembers(); i++) {
			ExecutionGroupVertex execGroupVertex = stage.getStageMember(i);
			if (execGroupVertex.getJobVertexID().equals(
					this.jobInputVertex.getID()))
				this.execInputVertex = execGroupVertex;
			else if (execGroupVertex.getJobVertexID().equals(
					this.jobTaskVertex.getID()))
				this.execTaskVertex = execGroupVertex;
			else if (execGroupVertex.getJobVertexID().equals(
					this.jobOutputVertex.getID()))
				this.execOutputVertex = execGroupVertex;
		}

		this.instanceConnectionInfos = new InstanceConnectionInfo[3][];
		this.instanceConnectionInfos[0] = this
				.createAndAssignDisjointConnectionInfos(this.execInputVertex);
		this.instanceConnectionInfos[1] = this
				.createAndAssignDisjointConnectionInfos(this.execTaskVertex);
		this.instanceConnectionInfos[2] = this
				.createAndAssignDisjointConnectionInfos(this.execOutputVertex);

		this.executionGraph2 = new ExecutionGraph(this.jobGraph2,
				mockInstanceManager);
		ExecutionStage stage2 = this.executionGraph2.getStage(0);
		for (int i = 0; i < stage2.getNumberOfStageMembers(); i++) {
			ExecutionGroupVertex executionGroupVertex = stage2
					.getStageMember(i);
			if (executionGroupVertex.getJobVertexID().equals(
					this.jobInputVertex2.getID()))
				this.execInputVertex2 = executionGroupVertex;
			else if (executionGroupVertex.getJobVertexID().equals(
					this.jobTaskVertex2.getID()))
				this.execTaskVertex2 = executionGroupVertex;
			else if (executionGroupVertex.getJobVertexID().equals(
					this.jobOutputVertex2.getID()))
				this.execOutputVertex2 = executionGroupVertex;
		}

		this.instanceConnectionInfos2 = new InstanceConnectionInfo[3][];
		this.instanceConnectionInfos2[0] = this
				.createAndAssignDisjointConnectionInfos(this.execInputVertex2);
		this.instanceConnectionInfos2[1] = this
				.createAndAssignDisjointConnectionInfos(this.execTaskVertex2);
		this.instanceConnectionInfos2[2] = this
				.createAndAssignDisjointConnectionInfos(this.execOutputVertex2);
	}

	private void makeConstraints() throws IOException {
		ConstraintUtil.defineAllLatencyConstraintsBetween(this.jobInputVertex,
				this.jobOutputVertex, 2000l);
		this.constraintFull = ConstraintUtil.getConstraints(
				this.jobGraph.getJobConfiguration()).get(0);
		this.constraints = new HashMap<LatencyConstraintID, QosGraph>();
		for (JobGraphLatencyConstraint constraint : ConstraintUtil
				.getConstraints(this.jobGraph.getJobConfiguration())) {

			this.constraints
					.put(constraint.getID(), QosGraphFactory
							.createConstrainedQosGraph(this.executionGraph,
									constraint));
		}

		ConstraintUtil.defineAllLatencyConstraintsBetween(this.jobInputVertex2,
				this.jobOutputVertex2, 2000l);
		this.constraints2 = new HashMap<LatencyConstraintID, QosGraph>();
		for (JobGraphLatencyConstraint constraint : ConstraintUtil
				.getConstraints(this.jobGraph2.getJobConfiguration())) {

			this.constraints2
					.put(constraint.getID(), QosGraphFactory
							.createConstrainedQosGraph(this.executionGraph2,
									constraint));
		}
	}

	/**
	 * The method will iterate over the members of an
	 * {@link ExecutionGroupVertex} and assign a unique
	 * {@link InstanceConnectionInfo} to each member.
	 * 
	 * @param groupVertex
	 *            the group vertex to be processed
	 * @return an array of these {@link InstanceConnectionInfo}s
	 */
	private InstanceConnectionInfo[] createAndAssignDisjointConnectionInfos(
			ExecutionGroupVertex groupVertex) {

		InstanceConnectionInfo[] ci = new InstanceConnectionInfo[groupVertex
				.getCurrentNumberOfGroupMembers()];
		for (int i = 0; i < ci.length; i++) {
			String host = String.format("10.10.10.%d", i + 1);
			try {
				ci[i] = new InstanceConnectionInfo(InetAddress.getByName(host),
						String.format("host%d", i), "domain", 1, 1);
				AbstractInstance inst = mock(AbstractInstance.class);
				when(inst.getInstanceConnectionInfo()).thenReturn(ci[i]);
				AllocatedResource resource = mock(AllocatedResource.class);
				when(resource.getInstance()).thenReturn(inst);
				groupVertex.getGroupMember(i).setAllocatedResource(resource);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		return ci;
	}

	/**
	 * TODO use util-class when it's available (post merge with master)
	 */
	public static class DummyRecord implements Record {

		@Override
		public void write(DataOutput out) throws IOException {

		}

		@Override
		public void read(DataInput in) throws IOException {

		}
	}

	public static class DummyInputTask extends AbstractGenericInputTask {

		@Override
		public void registerInputOutput() {
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {

		}
	}

	public static class DummyOutputTask extends AbstractOutputTask {

		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {

		}
	}

	public static class DummyTask extends AbstractTask {

		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {

		}
	}
}
