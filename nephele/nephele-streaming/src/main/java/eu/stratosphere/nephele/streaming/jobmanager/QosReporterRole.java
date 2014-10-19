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
package eu.stratosphere.nephele.streaming.jobmanager;

import eu.stratosphere.nephele.streaming.SamplingStrategy;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

import java.util.HashSet;
import java.util.Set;

/**
 * Models a Qos reporter role while computing the Qos setup on the job manager
 * side. A Qos reporter role is defined by a reporting action (REPORT_TASK_STATS
 * or REPORT_CHANNEL_STATS), the affected QosVertex/gate combination or QosEdge
 * and the set of task managers that have QosManager roles. A Qos reporter is
 * not directly associated with a constraint (only indirectly via the Qos
 * managers).
 * 
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosReporterRole {

	public enum ReportingAction {
		REPORT_TASK_STATS, REPORT_CHANNEL_STATS
	}

	private HashSet<QosManagerRole> targetQosManagers;

	private ReportingAction action;

	private QosVertex vertex;

	private int inputGateIndex;

	private int outputGateIndex;

	private SamplingStrategy samplingStrategy;

	private QosEdge edge;

	private QosReporterID reporterID;

	/**
	 * Creates a new Qos reporter role for a vertex, that reports to a single
	 * Qos manager.
	 */
	public QosReporterRole(QosVertex vertex, int inputGateIndex,
			int outputGateIndex, SamplingStrategy samplingStrategy, QosManagerRole targetQosManager) {

		this.action = ReportingAction.REPORT_TASK_STATS;
		this.vertex = vertex;
		this.targetQosManagers = new HashSet<QosManagerRole>(2);
		this.targetQosManagers.add(targetQosManager);
		this.inputGateIndex = inputGateIndex;
		this.outputGateIndex = outputGateIndex;
		this.samplingStrategy = samplingStrategy;
		this.reporterID = this.createReporterRoleID();
	}

	/**
	 * Creates a new Qos reporter role for an edge, that reports to a single Qos
	 * manager.
	 */
	public QosReporterRole(QosEdge edge, QosManagerRole targetQosManager) {

		this.action = ReportingAction.REPORT_CHANNEL_STATS;
		this.edge = edge;
		this.targetQosManagers = new HashSet<QosManagerRole>(2);
		this.targetQosManagers.add(targetQosManager);
		this.reporterID = this.createReporterRoleID();
	}

	private QosReporterID createReporterRoleID() {
		if (this.action == ReportingAction.REPORT_CHANNEL_STATS) {
			return QosReporterID.forEdge(this.edge.getSourceChannelID());
		}
		return QosReporterID.forVertex(
				this.vertex.getID(),
				this.inputGateIndex != -1 ? this.vertex.getInputGate(
						this.inputGateIndex).getGateID() : null,
				this.outputGateIndex != -1 ? this.vertex.getOutputGate(
						this.outputGateIndex).getGateID() : null);
	}

	public void merge(QosReporterRole otherRole) {
		if (!this.reporterID.equals(otherRole.reporterID)) {
			throw new RuntimeException("Cannot merge unequal QosReporter roles");
		}
		this.targetQosManagers.addAll(otherRole.targetQosManagers);
	}

	/**
	 * Returns the targetQosManagers.
	 * 
	 * @return the targetQosManagers
	 */
	public Set<QosManagerRole> getTargetQosManagers() {
		return this.targetQosManagers;
	}

	/**
	 * Returns the action.
	 * 
	 * @return the action
	 */
	public ReportingAction getAction() {
		return this.action;
	}

	/**
	 * Returns the vertex.
	 * 
	 * @return the vertex
	 */
	public QosVertex getVertex() {
		return this.vertex;
	}

	/**
	 * Returns the inputGateIndex.
	 * 
	 * @return the inputGateIndex
	 */
	public int getInputGateIndex() {
		return this.inputGateIndex;
	}

	/**
	 * Returns the outputGateIndex.
	 * 
	 * @return the outputGateIndex
	 */
	public int getOutputGateIndex() {
		return this.outputGateIndex;
	}


	/**
	 * Return the sampling strategy for the vertex.
	 *
	 * @return the sampling strategy
	 */
	public SamplingStrategy getSamplingStrategy() {
		return samplingStrategy;
	}

	/**
	 * Returns the edge.
	 * 
	 * @return the edge
	 */
	public QosEdge getEdge() {
		return this.edge;
	}

	/**
	 * Returns the reporterID.
	 * 
	 * @return the reporterID
	 */
	public QosReporterID getReporterID() {
		return this.reporterID;
	}
}
