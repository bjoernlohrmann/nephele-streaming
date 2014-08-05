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
package eu.stratosphere.nephele.streaming.message.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;

/**
 * Informs a task manager about its Qos manager role.
 * 
 * @author Bjoern Lohrmann / Sascha Wolke
 * 
 */
public class DeployInstanceQosManagerRoleAction extends AbstractSerializableQosMessage implements QosAction {

	private final InstanceConnectionInfo instanceConnectionInfo;

	private QosManagerConfig qosManager;

	public DeployInstanceQosManagerRoleAction() {
		this.instanceConnectionInfo = new InstanceConnectionInfo();
	}

	public DeployInstanceQosManagerRoleAction(JobID jobID,
			InstanceConnectionInfo instanceConnectionInfo) {
		super(jobID);
		this.instanceConnectionInfo = instanceConnectionInfo;
	}

	public InstanceConnectionInfo getInstanceConnectionInfo() {
		return this.instanceConnectionInfo;
	}

	public void setQosManager(QosManagerConfig qosManager) {
		this.qosManager = qosManager;
	}

	public QosManagerConfig getQosManager() {
		return this.qosManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		this.instanceConnectionInfo.write(out);
		this.qosManager.write(out);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.instanceConnectionInfo.read(in);
		this.qosManager = new QosManagerConfig();
		this.qosManager.read(in);
	}
}
