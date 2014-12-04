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
package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.message.ChainUpdates;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.action.VertexQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeLatency;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeStatistics;
import eu.stratosphere.nephele.streaming.message.qosreport.QosReport;
import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeQosData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGate;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.VertexQosData;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;

/**
 * Wrapper class around a Qos graph used by a Qos manager. A Qos model is a
 * state machine that first assembles a Qos graph from
 * {@link EdgeQosReporterConfig} and {@link VertexQosReporterConfig} objects and
 * then continuously adds Qos report data to the Qos graph. It can then be used
 * to search for violated Qos constraints inside the Qos graph.
 * 
 * @author Bjoern Lohrmann
 */
public class QosModel {

	public enum State {
		/**
		 * If the Qos model is empty, it means that the internal Qos graph does
		 * not contain any group vertices.
		 */
		EMPTY,

		/**
		 * If the Qos model is shallow, it means that the internal Qos graph
		 * does contain group vertices, but at least one group vertex has no
		 * members. Members are added by vertex/edge announcements piggybacked
		 * inside of Qos reports from the Qos reporters.
		 */
		SHALLOW,

		/**
		 * If the Qos model is ready, it means that the internal Qos graph does
		 * contain group vertices, and each group vertex has at least one member
		 * vertex. A transition back to SHALLOW is possible, when new shallow
		 * group vertices are merged into the Qos graph. Members may still be
		 * added by vertex/edge announcements piggybacked inside of Qos reports
		 * at any time.
		 */
		READY
	}

	private State state;

	/**
	 * A sparse graph that is assembled from two sources: (1) The (shallow)
	 * group-level Qos graphs received as part of the Qos manager roles
	 * delivered by job manager. (2) The vertex/edge reporter announcements
	 * delivered by (possibly many) Qos reporters, once the vertex/edge produces
	 * Qos data (which is may never happen, especially for some edges).
	 */
	private QosGraph qosGraph;

	/**
	 * A dummy Qos report that buffers vertex/edge announcements for later
	 * processing.
	 */
	private final QosReport announcementBuffer;

	/**
	 * A dummy object containing chain updates that need to be buffered, because
	 * the edges affected by the update are not yet in the QoS graph.
	 */
	private final ChainUpdates chainUpdatesBuffer;

	/**
	 * All gates of the Qos graph mapped by their ID.
	 */
	private HashMap<GateID, QosGate> gatesByGateId;

	/**
	 * All Qos vertices of the Qos graph mapped by their ID.
	 */
	private HashMap<ExecutionVertexID, QosVertex> vertexByID;

	/**
	 * All Qos edges of the Qos graph mapped by their source channel ID.
	 */
	private HashMap<ChannelID, QosEdge> edgeBySourceChannelID;

	public QosModel(JobID jobID) {
		this.state = State.EMPTY;
		this.announcementBuffer = new QosReport(jobID);
		this.chainUpdatesBuffer = new ChainUpdates(jobID);
		this.gatesByGateId = new HashMap<GateID, QosGate>();
		this.vertexByID = new HashMap<ExecutionVertexID, QosVertex>();
		this.edgeBySourceChannelID = new HashMap<ChannelID, QosEdge>();
	}

	public void mergeShallowQosGraph(QosGraph shallowQosGraph) {
		if (this.qosGraph == null) {
			this.qosGraph = shallowQosGraph;
		} else {
			this.qosGraph.merge(shallowQosGraph);
		}

		this.tryToProcessBufferedAnnouncements();
	}

	public boolean isReady() {
		return this.state == State.READY;
	}

	public boolean isEmpty() {
		return this.state == State.EMPTY;
	}

	public boolean isShallow() {
		return this.state == State.SHALLOW;
	}

	public void processQosReport(QosReport report) {
		switch (this.state) {
		case READY:
			if (report.hasAnnouncements()
					|| this.announcementBuffer.hasAnnouncements()) {
				this.bufferAndTryToProcessAnnouncements(report);
			}
			this.processQosRecords(report);
			break;
		case SHALLOW:
			this.bufferAndTryToProcessAnnouncements(report);
			break;
		case EMPTY:
			this.bufferAnnouncements(report);
			break;
		}
	}

	public void processChainUpdates(ChainUpdates announce) {
		this.bufferChainUpdates(announce);
		this.tryToProcessBufferedChainUpdates();
	}

	private void tryToProcessBufferedChainUpdates() {
		Iterator<QosReporterID.Edge> unchainedIter = this.chainUpdatesBuffer
				.getUnchainedEdges().iterator();
		while (unchainedIter.hasNext()) {

			QosReporterID.Edge edgeReporterID = unchainedIter.next();
			QosEdge edge = this.edgeBySourceChannelID.get(edgeReporterID
					.getSourceChannelID());

			if (edge == null) {
				continue;
			}

			edge.getQosData().setIsInChain(false);

			unchainedIter.remove();
			Logger.getLogger(this.getClass()).info(
					"Edge " + edge + " has been unchained.");
		}

		Iterator<QosReporterID.Edge> newlyChainedIter = this.chainUpdatesBuffer
				.getNewlyChainedEdges().iterator();
		while (newlyChainedIter.hasNext()) {

			QosReporterID.Edge edgeReporterID = newlyChainedIter.next();

			QosEdge edge = this.edgeBySourceChannelID.get(edgeReporterID
					.getSourceChannelID());

			if (edge == null) {
				continue;
			}

			edge.getQosData().setIsInChain(true);

			newlyChainedIter.remove();
			Logger.getLogger(this.getClass()).info(
					"Edge " + edge + " has been chained.");
		}
	}

	private void bufferChainUpdates(ChainUpdates announce) {
		this.chainUpdatesBuffer.getUnchainedEdges().addAll(
				announce.getUnchainedEdges());
		this.chainUpdatesBuffer.getNewlyChainedEdges().addAll(
				announce.getNewlyChainedEdges());
	}

	private void processQosRecords(QosReport report) {
		long now = System.currentTimeMillis();
		this.processVertexStatistics(report.getVertexStatistics(), now);
		this.processEdgeStatistics(report.getEdgeStatistics(), now);
		this.processEdgeLatencies(report.getEdgeLatencies(), now);
	}

	private void processVertexStatistics(
			Collection<VertexStatistics> vertexLatencies, long now) {

		for (VertexStatistics vertexStats : vertexLatencies) {
			QosReporterID.Vertex reporterID = vertexStats.getReporterID();

			QosVertex qosVertex = this.vertexByID.get(reporterID.getVertexID());

			int inputGateIndex = -1;
			if (reporterID.getInputGateID() != null) {
				inputGateIndex = this.gatesByGateId.get(
						reporterID.getInputGateID()).getGateIndex();
			}

			int outputGateIndex = -1;
			if (reporterID.getOutputGateID() != null) {
				outputGateIndex = this.gatesByGateId.get(
						reporterID.getOutputGateID()).getGateIndex();
			}

			if (qosVertex != null) {
				VertexQosData qosData = qosVertex.getQosData();
				qosData.addVertexStatisticsMeasurement(inputGateIndex,
						outputGateIndex, now, vertexStats);
			}
		}
	}

	private void processEdgeStatistics(
			Collection<EdgeStatistics> edgeStatistics, long now) {
		for (EdgeStatistics edgeStatistic : edgeStatistics) {
			QosReporterID.Edge reporterID = edgeStatistic.getReporterID();
			QosEdge edge = this.edgeBySourceChannelID.get(reporterID
					.getSourceChannelID());

			if (edge != null) {
				edge.getQosData().addOutputChannelStatisticsMeasurement(now,
						edgeStatistic);
			}
		}
	}

	private void processEdgeLatencies(Collection<EdgeLatency> edgeLatencies,
			long now) {

		for (EdgeLatency edgeLatency : edgeLatencies) {
			QosReporterID.Edge reporterID = edgeLatency.getReporterID();
			QosEdge edge = this.edgeBySourceChannelID.get(reporterID
					.getSourceChannelID());

			if (edge != null) {
				edge.getQosData().addLatencyMeasurement(now,
						edgeLatency.getEdgeLatency());
			}
		}
	}

	private void bufferAndTryToProcessAnnouncements(QosReport report) {
		this.bufferAnnouncements(report);
		this.tryToProcessBufferedAnnouncements();
		this.tryToProcessBufferedChainUpdates();
	}

	private void tryToProcessBufferedAnnouncements() {
		this.tryToProcessBufferedVertexReporterAnnouncements();
		this.tryToProcessBufferedEdgeReporterAnnouncements();

		if (this.qosGraph.isShallow()) {
			this.state = State.SHALLOW;
		} else {
			this.state = State.READY;
		}
	}

	private void tryToProcessBufferedEdgeReporterAnnouncements() {
		Iterator<EdgeQosReporterConfig> vertexIter = this.announcementBuffer
				.getEdgeQosReporterAnnouncements().iterator();

		while (vertexIter.hasNext()) {
			EdgeQosReporterConfig toProcess = vertexIter.next();

			QosGate outputGate = this.gatesByGateId.get(toProcess
					.getOutputGateID());
			QosGate inputGate = this.gatesByGateId.get(toProcess
					.getInputGateID());

			if (inputGate != null && outputGate != null) {
				this.assembleQosEdgeFromReporterConfig(toProcess, outputGate,
						inputGate);
				vertexIter.remove();
			}
		}
	}

	private void assembleQosEdgeFromReporterConfig(
			EdgeQosReporterConfig toProcess, QosGate outputGate,
			QosGate inputGate) {

		if (this.edgeBySourceChannelID.get(toProcess.getSourceChannelID()) == null) {
			QosEdge edge = toProcess.toQosEdge();
			edge.setOutputGate(outputGate);
			edge.setInputGate(inputGate);
			edge.setQosData(new EdgeQosData(edge));
			this.edgeBySourceChannelID.put(edge.getSourceChannelID(), edge);
		}
	}

	private void tryToProcessBufferedVertexReporterAnnouncements() {
		Iterator<VertexQosReporterConfig> vertexIter = this.announcementBuffer
				.getVertexQosReporterAnnouncements().iterator();

		while (vertexIter.hasNext()) {
			VertexQosReporterConfig toProcess = vertexIter.next();

			QosGroupVertex groupVertex = this.qosGraph
					.getGroupVertexByID(toProcess.getGroupVertexID());

			if (groupVertex != null) {
				this.assembleQosVertexFromReporterConfig(toProcess, groupVertex);
				vertexIter.remove();
			}
		}
	}

	/**
	 * Assembles a member vertex for the given group vertex, using the reporter
	 * config data.
	 */
	private void assembleQosVertexFromReporterConfig(
			VertexQosReporterConfig toProcess, QosGroupVertex groupVertex) {

		int memberIndex = toProcess.getMemberIndex();
		QosVertex memberVertex = groupVertex.getMember(memberIndex);

		// if the reporter config has a previously unknown member
		// vertex, add it to the group vertex
		if (memberVertex == null) {
			memberVertex = toProcess.toQosVertex();
			memberVertex.setQosData(new VertexQosData(memberVertex));
			groupVertex.setGroupMember(memberVertex);
			this.vertexByID.put(memberVertex.getID(), memberVertex);
		}

		int inputGateIndex = toProcess.getInputGateIndex();
		int outputGateIndex = toProcess.getOutputGateIndex();

		// if the reporter config has a previously unknown input gate
		// for us, add it to the vertex
		if (inputGateIndex != -1
				&& memberVertex.getInputGate(inputGateIndex) == null) {

			QosGate gate = toProcess.toInputGate();
			memberVertex.setInputGate(gate);
			this.gatesByGateId.put(gate.getGateID(), gate);
		}

		// if the reporter config has a previously unknown output gate
		// for us, add it to the vertex
		if (outputGateIndex != -1
				&& memberVertex.getOutputGate(outputGateIndex) == null) {

			QosGate gate = toProcess.toOutputGate();
			memberVertex.setOutputGate(gate);
			this.gatesByGateId.put(gate.getGateID(), gate);
		}

		// only if the reporter has a valid input/output gate combination,
		// prepare for reports on that combination
		if (inputGateIndex != -1 && outputGateIndex != -1) {
			memberVertex.getQosData().prepareForReportsOnGateCombination(
					inputGateIndex, outputGateIndex);
		} else if (inputGateIndex != -1) {
			memberVertex.getQosData().prepareForReportsOnInputGate(
					inputGateIndex);
		} else {
			memberVertex.getQosData().prepareForReportsOnOutputGate(
					outputGateIndex);
		}
	}

	private void bufferAnnouncements(QosReport report) {
		// bufferEdgeLatencies(report.getEdgeLatencies());
		// bufferEdgeStatistics(report.getEdgeStatistics());
		// bufferVertexLatencies(report.getVertexLatencies());
		this.bufferEdgeQosReporterAnnouncements(report
				.getEdgeQosReporterAnnouncements());
		this.bufferVertexQosReporterAnnouncements(report
				.getVertexQosReporterAnnouncements());
	}

	private void bufferVertexQosReporterAnnouncements(
			Collection<VertexQosReporterConfig> vertexQosReporterAnnouncements) {

		for (VertexQosReporterConfig reporterConfig : vertexQosReporterAnnouncements) {
			this.announcementBuffer.announceVertexQosReporter(reporterConfig);
		}
	}

	private void bufferEdgeQosReporterAnnouncements(
			List<EdgeQosReporterConfig> edgeQosReporterAnnouncements) {

		for (EdgeQosReporterConfig reporterConfig : edgeQosReporterAnnouncements) {
			this.announcementBuffer
					.addEdgeQosReporterAnnouncement(reporterConfig);
		}
	}

	public List<QosConstraintSummary> findQosConstraintViolationsAndSummarize(
			QosConstraintViolationListener listener) {
		
		long now = System.currentTimeMillis();

		long inactivityThresholdTime = now - 2
				* StreamPluginConfig.getAdjustmentIntervalMillis();

		List<QosConstraintSummary> constraintSummaries = new LinkedList<QosConstraintSummary>();

		for (JobGraphLatencyConstraint constraint : this.qosGraph
				.getConstraints()) {

			QosConstraintViolationFinder constraintViolationFinder = new QosConstraintViolationFinder(
					constraint.getID(), this.qosGraph, listener,
					inactivityThresholdTime);

			QosConstraintViolationReport violationReport = constraintViolationFinder
					.scanSequencesForQosConstraintViolations();

			constraintSummaries.add(createConstraintSummary(constraint,
					violationReport, inactivityThresholdTime));
		}

		return constraintSummaries;
	}

	private QosConstraintSummary createConstraintSummary(
			JobGraphLatencyConstraint constraint,
			QosConstraintViolationReport violationReport,
			long inactivityThresholdTime) {

		QosConstraintSummary constraintSummary = new QosConstraintSummary(
				constraint, violationReport);

		JobGraphSequence seq = constraint.getSequence();

		for (SequenceElement seqElem : seq) {
			if (seqElem.isVertex()) {
				summarizeGroupVertex(seqElem,
						constraintSummary.getGroupVertexSummary(seqElem
								.getIndexInSequence()), inactivityThresholdTime);

			} else {
				summarizeGroupEdge(seqElem,
						constraintSummary.getGroupEdgeSummary(seqElem
								.getIndexInSequence()), inactivityThresholdTime);
			}
		}

		fixupGroupEdgeSummaries(constraint, constraintSummary,
				inactivityThresholdTime);

		return constraintSummary;
	}

	private void fixupGroupEdgeSummaries(JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary, long inactivityThresholdTime) {

		JobGraphSequence seq = constraint.getSequence();

		for (SequenceElement seqElem : seq) {
			if (seqElem.isEdge()) {
				int succIndex = seqElem.getIndexInSequence() + 1;
				QosGroupEdgeSummary toFix = constraintSummary
						.getGroupEdgeSummary(seqElem.getIndexInSequence());
				
				QosGroupVertexSummary succSummary;

				if (succIndex < seq.size()) {
					succSummary = constraintSummary
							.getGroupVertexSummary(succIndex);

				} else {
					succSummary = new QosGroupVertexSummary();
					summarizeGroupVertex(succSummary, inactivityThresholdTime,
							seqElem.getInputGateIndex(), -1,
							qosGraph.getGroupVertexByID(seqElem
									.getTargetVertexID()));
				}
				
				toFix.setMeanConsumerVertexLatency(succSummary
						.getMeanVertexLatency());
				toFix.setMeanConsumerVertexLatencyCV(succSummary
						.getMeanVertexLatencyCV());
			} 
		}
	}

	private void summarizeGroupVertex(SequenceElement seqElem,
			QosGroupVertexSummary groupVertexSummary,
			long inactivityThresholdTime) {

		int inputGateIndex = seqElem.getInputGateIndex();
		int outputGateIndex = seqElem.getInputGateIndex();
		QosGroupVertex groupVertex = qosGraph.getGroupVertexByID(seqElem
				.getVertexID());

		summarizeGroupVertex(groupVertexSummary, inactivityThresholdTime,
				inputGateIndex, outputGateIndex, groupVertex);
	}

	private void summarizeGroupVertex(QosGroupVertexSummary groupVertexSummary,
			long inactivityThresholdTime, int inputGateIndex,
			int outputGateIndex, QosGroupVertex groupVertex) {

		int activeVertices = 0;
		double vertexLatencySum = 0;
		double vertexLatencyCASum = 0;

		for (QosVertex memberVertex : groupVertex.getMembers()) {
			VertexQosData qosData = memberVertex.getQosData();

			if (qosData.hasNewerData(inputGateIndex, outputGateIndex,
					inactivityThresholdTime)) {
				activeVertices++;
				vertexLatencySum += qosData.getLatencyInMillis(inputGateIndex);
				vertexLatencyCASum += qosData.getLatencyCV(inputGateIndex);
			}
		}

		if (activeVertices > 0) {
			groupVertexSummary.setActiveVertices(activeVertices);
			groupVertexSummary.setMeanVertexLatency(vertexLatencySum
					/ activeVertices);
			groupVertexSummary
					.setMeanVertexLatencyCV(vertexLatencyCASum
							/ activeVertices);
		}
	}

	private void summarizeGroupEdge(SequenceElement seqElem,
			QosGroupEdgeSummary groupEdgeSummary, long inactivityThresholdTime) {

		int activeEdges = 0;
		double outputBufferLatencySum = 0;
		double transportLatencySum = 0;

		int activeConsumerVertices = 0;
		double consumptionRateSum = 0;
		double interarrivalTimeSum = 0;
		double interarrivalTimeCASum = 0;

		int inputGateIndex = seqElem.getInputGateIndex();
		QosGroupVertex targetGroupVertex = qosGraph.getGroupVertexByID(seqElem
				.getTargetVertexID());

		for (QosVertex memberVertex : targetGroupVertex.getMembers()) {
			VertexQosData qosData = memberVertex.getQosData();

			if (qosData.hasNewerData(inputGateIndex, -1,
					inactivityThresholdTime)) {
				activeConsumerVertices++;
				consumptionRateSum += qosData
						.getRecordsConsumedPerSec(inputGateIndex);
				interarrivalTimeSum += qosData
						.getInterArrivalTimeInMillis(inputGateIndex);
				interarrivalTimeCASum += qosData
						.getInterArrivalTimeCV(inputGateIndex);
			}

			for (QosEdge ingoingEdge : memberVertex
					.getInputGate(inputGateIndex).getEdges()) {
				EdgeQosData edgeQosData = ingoingEdge.getQosData();

				if (edgeQosData.hasNewerData(inactivityThresholdTime)) {
					activeEdges++;
					outputBufferLatencySum += edgeQosData
							.estimateOutputBufferLatencyInMillis();
					transportLatencySum += edgeQosData
							.estimateTransportLatencyInMillis();
				}
			}
		}

		if (activeEdges > 0 && activeConsumerVertices > 0) {
			groupEdgeSummary.setActiveEdges(activeEdges);
			groupEdgeSummary.setOutputBufferLatencyMean(outputBufferLatencySum
					/ activeEdges);
			groupEdgeSummary.setTransportLatencyMean(transportLatencySum
					/ activeEdges);

			groupEdgeSummary.setActiveConsumerVertices(activeConsumerVertices);
			groupEdgeSummary.setMeanConsumptionRate(consumptionRateSum
					/ activeConsumerVertices);
			groupEdgeSummary
					.setMeanConsumerVertexInterarrivalTime(interarrivalTimeSum
							/ activeConsumerVertices);
			groupEdgeSummary
					.setMeanConsumerVertexInterarrivalTimeCV(interarrivalTimeCASum
							/ activeConsumerVertices);
			setSourceGroupVertexEmissionRate(seqElem, inactivityThresholdTime,
					groupEdgeSummary);
		}
	}

	private void setSourceGroupVertexEmissionRate(SequenceElement seqElem,
			long inactivityThresholdTime, QosGroupEdgeSummary groupEdgeSummary) {

		int activeEmitterVertices = 0;
		double emissionRateSum = 0;

		int outputGateIndex = seqElem.getOutputGateIndex();
		QosGroupVertex sourceGroupVertex = qosGraph.getGroupVertexByID(seqElem
				.getSourceVertexID());

		for (QosVertex memberVertex : sourceGroupVertex.getMembers()) {
			VertexQosData qosData = memberVertex.getQosData();
			if (qosData.hasNewerData(-1, outputGateIndex,
					inactivityThresholdTime)) {
				activeEmitterVertices++;
				emissionRateSum += qosData
						.getRecordsEmittedPerSec(outputGateIndex);
			}
		}

		if (activeEmitterVertices > 0) {
			groupEdgeSummary.setActiveEmitterVertices(activeEmitterVertices);
			groupEdgeSummary.setMeanEmissionRate(emissionRateSum
					/ activeEmitterVertices);
		}
	}

	public JobGraphLatencyConstraint getJobGraphLatencyConstraint(
			LatencyConstraintID constraintID) {
		return this.qosGraph.getConstraintByID(constraintID);
	}
	
	public Collection<JobGraphLatencyConstraint> getJobGraphLatencyConstraints() {
		return this.qosGraph.getConstraints();
	}

	public State getState() {
		return this.state;
	}
}
