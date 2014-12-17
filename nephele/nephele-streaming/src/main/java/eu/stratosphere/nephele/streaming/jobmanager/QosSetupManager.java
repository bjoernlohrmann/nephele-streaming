package eu.stratosphere.nephele.streaming.jobmanager;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGate;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.executiongraph.VertexAssignmentListener;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.AbstractJobInputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.StreamingPluginLoader;
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.ElasticTaskQosAutoScalingThread;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosManagerRoleAction;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.message.action.DestroyInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFactory;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.WrapperUtils;
import eu.stratosphere.nephele.streaming.util.StreamUtil;
import eu.stratosphere.nephele.taskmanager.runtime.ExecutorThreadFactory;

/**
 * This class coordinates Qos setup computation on the job manager for a given
 * job. This class receives calls from the job manager directly between job
 * submission by the client and deployment on the task managers. It analyzes
 * which constraints have been attached to the job graph and rewrites it
 * accordingly. Additionally, it waits for a complete allocation of execution
 * vertices to task managers and then triggers the actual computation of the Qos
 * setup.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosSetupManager implements VertexAssignmentListener {

	private static final Log LOG = LogFactory.getLog(QosSetupManager.class);

	private ExecutionGraph executionGraph;

	private ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance> taskManagers;

	private JobID jobID;

	private List<JobGraphLatencyConstraint> constraints;
	
	private HashMap<LatencyConstraintID, QosGraph> qosGraphs;

	private HashSet<JobVertexID> constrainedJobVertices;

	private HashSet<ExecutionVertexID> verticesWithPendingAllocation;
	
	private volatile ElasticTaskQosAutoScalingThread autoscalingThread;
	
	private QosSetup qosSetup;

	public QosSetupManager(JobID jobID,
			List<JobGraphLatencyConstraint> constraints) {
		this.jobID = jobID;
		this.constraints = constraints;
		this.constrainedJobVertices = this.computeJobVerticesToRewrite();
	}
	
	private HashMap<LatencyConstraintID, QosGraph> createQosGraphs() {
		HashMap<LatencyConstraintID, QosGraph> qosGraphs = new HashMap<LatencyConstraintID, QosGraph>();
		
		for (JobGraphLatencyConstraint constraint : this.constraints) {
			qosGraphs.put(constraint.getID(),
					QosGraphFactory.createConstrainedQosGraph(this.executionGraph,
									constraint));
		}
		
		return qosGraphs;
	}

	public void registerOnExecutionGraph(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;
		this.taskManagers = new ConcurrentHashMap<InstanceConnectionInfo, AbstractInstance>();
		this.attachAssignmentListenersToExecutionGraph();
	}

	private void attachAssignmentListenersToExecutionGraph() {
		this.verticesWithPendingAllocation = new HashSet<ExecutionVertexID>();
		Set<ExecutionVertexID> visited = new HashSet<ExecutionVertexID>();

		for (int i = 0; i < this.executionGraph.getNumberOfInputVertices(); i++) {
			ExecutionVertex inputVertex = this.executionGraph.getInputVertex(i);
			this.attachAssignmentListenersToReachableVertices(inputVertex,
					visited);
		}
	}

	private void attachAssignmentListenersToReachableVertices(
			ExecutionVertex vertex, Set<ExecutionVertexID> visited) {

		if (visited.contains(vertex.getID())) {
			return;
		}
		visited.add(vertex.getID());

		if (this.constrainedJobVertices.contains(vertex.getGroupVertex()
				.getJobVertexID())) {
			vertex.registerVertexAssignmentListener(this);
			this.verticesWithPendingAllocation.add(vertex.getID());
		}

		for (int i = 0; i < vertex.getNumberOfOutputGates(); i++) {
			ExecutionGate outputGate = vertex.getOutputGate(i);
			for (int j = 0; j < outputGate.getNumberOfEdges(); j++) {
				ExecutionVertex nextVertex = outputGate.getEdge(j)
						.getInputGate().getVertex();
				this.attachAssignmentListenersToReachableVertices(nextVertex,
						visited);
			}
		}
	}

	public void shutdown() {
		for (AbstractInstance instance : this.taskManagers.values()) {
			try {
				instance.sendData(StreamingPluginLoader.STREAMING_PLUGIN_ID,
						new DestroyInstanceQosRolesAction(this.jobID));
			} catch (IOException e) {
				LOG.warn("Failed to signal task manager qos setup shutdown: " + e.getMessage());
			}
		}
		this.taskManagers.clear();

		this.executionGraph = null;

		if (this.autoscalingThread != null) {
			this.autoscalingThread.shutdown();
			this.autoscalingThread = null;
		}

		this.qosSetup = null;
		this.qosGraphs = null;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public List<JobGraphLatencyConstraint> getConstraints() {
		return this.constraints;
	}

	/**
	 * Replaces the input/output/task classes of vertices inside the given job
	 * graph with wrapped versions. This is done only for the vertices that are
	 * affected by a constraint. The input/output/task classes contain the user
	 * defined code. This method wraps the user defined code and adds streaming
	 * framework calls that record latencies, can perform task chaining etc.
	 * 
	 * @param jobGraph
	 */
	public void rewriteJobGraph(JobGraph jobGraph) {
		this.rewriteInputVerticesWhereNecessary(jobGraph);
		this.rewriteTaskVerticesWhereNecessary(jobGraph);
		this.rewriteOutputVerticesWhereNecessary(jobGraph);
	}

	private HashSet<JobVertexID> computeJobVerticesToRewrite() {
		HashSet<JobVertexID> verticesToRewrite = new HashSet<JobVertexID>();

		for (JobGraphLatencyConstraint constraint : this.constraints) {
			verticesToRewrite.addAll(constraint.getSequence()
					.getVerticesInSequence());

			if (constraint.getSequence().getFirst().isEdge()) {
				verticesToRewrite.add(constraint.getSequence().getFirst()
						.getSourceVertexID());
			}

			if (constraint.getSequence().getLast().isEdge()) {
				verticesToRewrite.add(constraint.getSequence().getLast()
						.getTargetVertexID());
			}
		}

		return verticesToRewrite;
	}

	private void rewriteOutputVerticesWhereNecessary(JobGraph jobGraph) {

		for (AbstractJobOutputVertex vertex : StreamUtil.toIterable(jobGraph
				.getOutputVertices())) {

			if (!this.constrainedJobVertices.contains(vertex.getID())) {
				continue;
			}

			if (!(vertex instanceof JobOutputVertex)) {
				LOG.warn("Cannot wrap output vertex of type "
						+ vertex.getClass().getName() + ", skipping...");
				continue;
			}
			WrapperUtils.wrapOutputClass((JobOutputVertex) vertex);
		}
	}

	private void rewriteInputVerticesWhereNecessary(JobGraph jobGraph) {

		for (AbstractJobInputVertex inputVertex : StreamUtil
				.toIterable(jobGraph.getInputVertices())) {

			if (!this.constrainedJobVertices.contains(inputVertex.getID())) {
				continue;
			}

			if (!(inputVertex instanceof JobInputVertex)) {
				LOG.warn("Cannot wrap input vertex of type "
						+ inputVertex.getClass().getName() + ", skipping...");
				continue;
			}
			WrapperUtils.wrapInputClass((JobInputVertex) inputVertex);
		}
	}

	private void rewriteTaskVerticesWhereNecessary(JobGraph jobGraph) {

		for (JobTaskVertex vertex : StreamUtil.toIterable(jobGraph
				.getTaskVertices())) {
			if (!this.constrainedJobVertices.contains(vertex.getID())) {
				continue;
			}

			WrapperUtils.wrapTaskClass(vertex);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.executiongraph.VertexAssignmentListener#
	 * vertexAssignmentChanged
	 * (eu.stratosphere.nephele.executiongraph.ExecutionVertexID,
	 * eu.stratosphere.nephele.instance.AllocatedResource)
	 */
	@Override
	public synchronized void vertexAssignmentChanged(ExecutionVertexID id,
			AllocatedResource newAllocatedResource) {

		try {
			this.verticesWithPendingAllocation.remove(id);
			AbstractInstance instance = newAllocatedResource.getInstance();
			this.taskManagers.putIfAbsent(instance.getInstanceConnectionInfo(),
					instance);
			if (this.verticesWithPendingAllocation.isEmpty()) {
				this.computeAndDistributeQosSetup();
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
	}

	private void computeAndDistributeQosSetup() throws IOException, InterruptedException {
		qosGraphs = createQosGraphs();
		
		qosSetup = new QosSetup(this.qosGraphs);
		qosSetup.computeQosRoles();
		qosSetup.computeCandidateChains(this.executionGraph);
		
		distributeQosSetup();
	}
	
	private void distributeQosSetup() throws IOException, InterruptedException {

		ExecutorService threadPool = Executors.newFixedThreadPool(10,
				ExecutorThreadFactory.INSTANCE);

		for (final TaskManagerQosSetup instanceQosRoles : qosSetup
				.getQosRoles().values()) {

			threadPool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						AbstractInstance instance = taskManagers
								.get(instanceQosRoles.getConnectionInfo());
						
						if (instanceQosRoles.hasQosManagerRoles()) {
							DeployInstanceQosManagerRoleAction managerRole = instanceQosRoles
									.toManagerDeploymentAction(executionGraph
											.getJobID());

							instance.sendData(StreamingPluginLoader.STREAMING_PLUGIN_ID,
									managerRole);
						}
						
						DeployInstanceQosRolesAction reporterRoles = instanceQosRoles
								.toDeploymentAction(executionGraph.getJobID());
						instance.sendData(StreamingPluginLoader.STREAMING_PLUGIN_ID,
								reporterRoles);

					} catch (Exception e) {
						LOG.error("Error while distribution QoS roles to task managers",
								e);
					}
				}
			});
		}

		threadPool.shutdown();
		threadPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		LOG.info(String.format("Distributed QoS roles to %d task managers", qosSetup.getQosRoles().size()));
	}

	private void ensureElasticTaskAutoScalerIsRunning() {
		// this may seem like clunky code, however
		// this is a highly used code path. Reading a volatile
		// variable that is rarely written is very cheap, whereas obtaining an
		// object monitor (as done when calling a synchronized method) is not.
		if (this.autoscalingThread == null) {
			ensureElasticTaskAutoScalerIsRunningSynchronized();
		}
	}

	private synchronized void ensureElasticTaskAutoScalerIsRunningSynchronized() {
		if (this.autoscalingThread == null) {
			this.autoscalingThread = new ElasticTaskQosAutoScalingThread(
					executionGraph, qosGraphs, qosSetup.getQosManagerIDsByConstraint());
		}
	}

	public void handleMessage(AbstractSerializableQosMessage qosMessage) {
		ensureElasticTaskAutoScalerIsRunning();
		this.autoscalingThread.enqueueMessage(qosMessage);
	}
}
