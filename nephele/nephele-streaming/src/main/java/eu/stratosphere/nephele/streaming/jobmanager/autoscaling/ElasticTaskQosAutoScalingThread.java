package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.web.QosStatisticsServlet;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.message.AbstractQosMessage;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;
import eu.stratosphere.nephele.streaming.message.QosManagerConstraintSummaries;
import eu.stratosphere.nephele.streaming.message.TaskCpuLoadChange;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosLogger;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosManagerID;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;
import eu.stratosphere.nephele.streaming.web.QosJobWebStatistic;

public class ElasticTaskQosAutoScalingThread extends Thread {

	private static final Log LOG = LogFactory
			.getLog(ElasticTaskQosAutoScalingThread.class);

	private final JobID jobID;

	private final LinkedBlockingQueue<AbstractSerializableQosMessage> qosMessages = new LinkedBlockingQueue<AbstractSerializableQosMessage>();

	private long timeOfLastScaling;

	private long timeOfNextScaling;

	private final HashMap<ExecutionVertexID, TaskCpuLoadChange> taskCpuLoads = new HashMap<ExecutionVertexID, TaskCpuLoadChange>();

	private final HashMap<LatencyConstraintID, QosConstraintSummaryAggregator> aggregators = new HashMap<LatencyConstraintID, QosConstraintSummaryAggregator>();

	private final HashMap<LatencyConstraintID, LatencyConstraintCpuLoadSummaryAggregator> cpuLoadAggregators = new HashMap<LatencyConstraintID, LatencyConstraintCpuLoadSummaryAggregator>();

	private final HashMap<LatencyConstraintID, QosLogger> qosLoggers = new HashMap<LatencyConstraintID, QosLogger>();

	private final HashMap<LatencyConstraintID, CpuLoadLogger> cpuLoadLoggers = new HashMap<LatencyConstraintID, CpuLoadLogger>();

	private final QosJobWebStatistic webStatistic;

	private final HashMap<JobVertexID, Integer> vertexTopologicalScores = new HashMap<JobVertexID, Integer>();

	private AbstractScalingPolicy scalingPolicy;
	
	private ExecutorService threadPool = Executors.newSingleThreadExecutor();
	
	private Future<?> ongoingScalingActions = null;

	private int cooldownPeriods = 6;

	public ElasticTaskQosAutoScalingThread(ExecutionGraph execGraph,
			HashMap<LatencyConstraintID, QosGraph> qosGraphs,
			Set<QosManagerID> qosManagers) {

		this.setName("QosAutoScalingThread");
		this.jobID = execGraph.getJobID();
		this.timeOfLastScaling = 0;
		this.timeOfNextScaling = 0;

		long loggingInterval = StreamPluginConfig.getAdjustmentIntervalMillis();

		HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints = new HashMap<LatencyConstraintID, JobGraphLatencyConstraint>();

		for (LatencyConstraintID constraintID : qosGraphs.keySet()) {
			JobGraphLatencyConstraint constraint = qosGraphs.get(constraintID)
					.getConstraintByID(constraintID);

			qosConstraints.put(constraintID, constraint);
			aggregators.put(constraintID,
					new QosConstraintSummaryAggregator(execGraph, constraint, qosManagers));
			cpuLoadAggregators.put(constraintID,
					new LatencyConstraintCpuLoadSummaryAggregator(execGraph, constraint));

			try {
				qosLoggers.put(constraintID, new QosLogger(constraint, loggingInterval));
				cpuLoadLoggers.put(constraintID, new CpuLoadLogger(execGraph, constraint, loggingInterval));

			} catch (Exception e) {
				LOG.error("Exception while initiliazing loggers", e);
			}
		}

		scalingPolicy = new SimpleScalingPolicy(execGraph, qosConstraints);

		webStatistic = new QosJobWebStatistic(execGraph, loggingInterval, qosConstraints);
		QosStatisticsServlet.putStatistic(this.jobID, webStatistic);

		fillVertexTopologicalScores(qosGraphs);

		this.start();
	}

	private void fillVertexTopologicalScores(
			HashMap<LatencyConstraintID, QosGraph> qosGraphs) {

		QosGraph merged = new QosGraph();
		for (QosGraph qosGraph : qosGraphs.values()) {
			for (QosGroupVertex startVertex : qosGraph.getStartVertices()) {
				merged.mergeForwardReachableGroupVertices(startVertex, false);
			}
		}

		Map<JobVertexID, Integer> predecessorCounts = new HashMap<JobVertexID, Integer>();
		LinkedList<JobVertexID> verticesWithoutPredecessor = new LinkedList<JobVertexID>();

		for (QosGroupVertex groupVertex : merged.getAllVertices()) {
			int noOfPredecessors = groupVertex.getNumberOfInputGates();
			predecessorCounts.put(groupVertex.getJobVertexID(),
					noOfPredecessors);

			if (noOfPredecessors == 0) {
				verticesWithoutPredecessor.add(groupVertex.getJobVertexID());
			}
		}

		int nextTopoScore = 0;
		while (!verticesWithoutPredecessor.isEmpty()) {
			JobVertexID vertexWithoutPredecessor = verticesWithoutPredecessor
					.removeFirst();

			vertexTopologicalScores
					.put(vertexWithoutPredecessor, nextTopoScore);
			nextTopoScore++;

			for (QosGroupEdge forwardEdge : merged.getGroupVertexByID(
					vertexWithoutPredecessor).getForwardEdges()) {
				QosGroupVertex successor = forwardEdge.getTargetVertex();

				int newPredecessorCount = predecessorCounts.get(successor
						.getJobVertexID()) - 1;
				predecessorCounts.put(successor.getJobVertexID(),
						newPredecessorCount);
				if (newPredecessorCount == 0) {
					verticesWithoutPredecessor.add(successor.getJobVertexID());
				}
			}
		}
	}

		
	@Override
	public void run() {
		try {
			LOG.info("Qos Auto Scaling Thread started");
			
			long now = System.currentTimeMillis();

			while (!interrupted()) {
				processMessages();
				Thread.sleep(500);

				now = System.currentTimeMillis();

				if (scalingIsDue(now)) {
					List<QosConstraintSummary> constraintSummaries = aggregateConstraintSummaries();
					logConstraintSummaries(constraintSummaries);

					Map<LatencyConstraintID, LatencyConstraintCpuLoadSummary> cpuLoadSummaries = summarizeCpuUtilizations(taskCpuLoads);
					logCpuLoadSummaries(cpuLoadSummaries);

					if (cooldownPeriods > 0) {
						cooldownPeriods--;
					} else if (ongoingScalingActions != null) {
						if (ongoingScalingActions.isDone()) {
							ongoingScalingActions = null;
							cooldownPeriods = 3;
						}
					} else {
						Map<JobVertexID, Integer> scalingActions = scalingPolicy
								.getScalingActions(constraintSummaries,
										taskCpuLoads, cpuLoadSummaries);
						ongoingScalingActions = triggerScalingActions(scalingActions);
					}

					timeOfLastScaling = System.currentTimeMillis();
					timeOfNextScaling = timeOfLastScaling
							+ StreamPluginConfig.getAdjustmentIntervalMillis();
				}
			}
		} catch (InterruptedException e) {
			// do nothing
		} catch (UnexpectedVertexExecutionStateException e) {
			// do nothing, the job is usually finishing/canceling/failing
		} catch (Exception e) {
			LOG.error("Exception in auto scaling thread", e);
		} finally {
			cleanUp();
		}

		LOG.info("Qos Auto Scaling Thread stopped.");
	}

	private Future<?> triggerScalingActions(final Map<JobVertexID, Integer> scalingActions) {

		// apply scaling actions in topological order
		final JobVertexID[] topoSortedVertexIds = scalingActions.keySet().toArray(
				new JobVertexID[0]);

		Arrays.sort(topoSortedVertexIds, new Comparator<JobVertexID>() {
			@Override
			public int compare(JobVertexID first, JobVertexID second) {
				int firstTopoScore = vertexTopologicalScores.get(first);
				int secondTopoScore = vertexTopologicalScores.get(second);
				return Integer.compare(firstTopoScore, secondTopoScore);
			}
		});

		final JobManager jm = JobManager.getInstance();

		Runnable scaler = new Runnable() {
			@Override
			public void run() {
				try {
					for (JobVertexID vertexId : topoSortedVertexIds) {
						int scalingAction = scalingActions.get(vertexId);
						if (scalingAction > 0) {
							jm.scaleUpElasticTask(jobID, vertexId,
									scalingAction);
						}
					}

					for (JobVertexID vertexId : topoSortedVertexIds) {
						int scalingAction = scalingActions.get(vertexId);
						if (scalingAction < 0) {
							jm.scaleDownElasticTask(jobID, vertexId,
									scalingAction);

						}
					}
				} catch (Exception e) {
					LOG.error("Error during scaling action", e);
				}
			}
		};
		
		return threadPool.submit(scaler);
	}

	private List<QosConstraintSummary> aggregateConstraintSummaries() {
		LinkedList<QosConstraintSummary> toReturn = new LinkedList<QosConstraintSummary>();

		for (QosConstraintSummaryAggregator aggregator : aggregators.values()) {
			toReturn.add(aggregator.computeAggregation());
		}

		return toReturn;
	}

	private void logConstraintSummaries(
			List<QosConstraintSummary> constraintSummaries) {

		for (QosConstraintSummary constraintSummary : constraintSummaries) {
			QosLogger logger = qosLoggers.get(constraintSummary
					.getLatencyConstraintID());

			if (logger != null) {
				try {
					logger.logSummary(constraintSummary);

				} catch (IOException e) {
					LOG.error("Error during QoS logging", e);
				}
			}
		}

		webStatistic.logConstraintSummaries(constraintSummaries);
	}

	private Map<LatencyConstraintID, LatencyConstraintCpuLoadSummary> summarizeCpuUtilizations(Map<ExecutionVertexID, TaskCpuLoadChange> taskCpuLoads)
			throws UnexpectedVertexExecutionStateException {

		HashMap<LatencyConstraintID, LatencyConstraintCpuLoadSummary> summaries = new HashMap<LatencyConstraintID, LatencyConstraintCpuLoadSummary>();

		for (LatencyConstraintID constraint : this.cpuLoadAggregators.keySet()) {
			LatencyConstraintCpuLoadSummaryAggregator aggregator = this.cpuLoadAggregators.get(constraint);
			summaries.put(constraint, aggregator.summarizeCpuUtilizations(taskCpuLoads));
		}

		return summaries;
	}

	private void logCpuLoadSummaries(Map<LatencyConstraintID, LatencyConstraintCpuLoadSummary> summaries) {
		this.webStatistic.logCpuLoadSummaries(summaries);

		for (LatencyConstraintID constraint : summaries.keySet()) {
			CpuLoadLogger logger = this.cpuLoadLoggers.get(constraint);

			if (logger != null) {
				try {
					logger.logCpuLoads(summaries.get(constraint));

				} catch (IOException e) {
					LOG.error("Error during cpu load logging", e);
				}
			}
		}
	}

	private void cleanUp() {
		for (QosLogger logger : qosLoggers.values()) {
			try {
				logger.close();
			} catch (IOException e) {
				LOG.warn("Failure while closing qos logger!", e);
			}
		}

		for (CpuLoadLogger logger : cpuLoadLoggers.values()) {
			try {
				logger.close();
			} catch (IOException e) {
				LOG.warn("Failure while closing cpu load logger!", e);
			}
		}

		// clear large memory structures
		qosMessages.clear();
		aggregators.clear();
		taskCpuLoads.clear();
		vertexTopologicalScores.clear();
		scalingPolicy = null;

		QosStatisticsServlet.removeJob(this.jobID);
		
		if (ongoingScalingActions != null) {
			threadPool.shutdown();
			try {
				threadPool.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				threadPool.shutdownNow();
			}
			threadPool = null;
		}
	}

	private boolean scalingIsDue(long now) {
		if (now < timeOfNextScaling) {
			return false;
		}

		for (QosConstraintSummaryAggregator summaryAggregator : aggregators
				.values()) {
			if (!summaryAggregator.canAggregate()) {
				return false;
			}
		}

		return true;
	}

	private void processMessages() {
		while (!qosMessages.isEmpty()) {
			AbstractQosMessage nextMessage = qosMessages.poll();
			if (nextMessage instanceof TaskCpuLoadChange) {
				handleTaskLoadStateChange((TaskCpuLoadChange) nextMessage);
			} else if (nextMessage instanceof QosManagerConstraintSummaries) {
				handleQosManagerConstraintSummaries((QosManagerConstraintSummaries) nextMessage);
			}
		}
	}

	private void handleQosManagerConstraintSummaries(
			QosManagerConstraintSummaries nextMessage) {

		for (QosConstraintSummary constraintSummary : nextMessage
				.getConstraintSummaries()) {

			LatencyConstraintID constraintID = constraintSummary
					.getLatencyConstraintID();

			aggregators.get(constraintID).add(nextMessage.getQosManagerID(),
					constraintSummary);
		}
	}

	private void handleTaskLoadStateChange(TaskCpuLoadChange msg) {
		this.taskCpuLoads.put(msg.getVertexId(), msg);
	}

	public void enqueueMessage(AbstractSerializableQosMessage message) {
		this.qosMessages.add(message);
	}

	public void shutdown() {
		this.interrupt();
	}
}
