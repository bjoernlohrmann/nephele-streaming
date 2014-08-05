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

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;

/**
 * Informs a task manager about job shutdown.
 * 
 * @author Sascha Wolke
 * 
 */
public class DestroyInstanceQosRolesAction extends AbstractSerializableQosMessage implements QosAction {

	public DestroyInstanceQosRolesAction() {
	}

	public DestroyInstanceQosRolesAction(JobID jobID) {
		super(jobID);
	}
}
