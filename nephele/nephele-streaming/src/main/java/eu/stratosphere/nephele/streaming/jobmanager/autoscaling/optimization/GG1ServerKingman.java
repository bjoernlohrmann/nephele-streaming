package eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization;

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;

/**
 * Implements the "standard" Kingman formula for G/G/1 queueing systems, which
 * is an approximation of queue waiting time.
 * 
 * @author Bjoern Lohrmann
 */
public class GG1ServerKingman extends GG1Server {
	
	public GG1ServerKingman(JobVertexID groupVertexID, int minSubtasks,
			int maxSubtasks, QosGroupEdgeSummary edgeSummary) {
		super(groupVertexID, minSubtasks, maxSubtasks, edgeSummary);
	}

	protected double getQueueWaitUnfitted(int newP, double rho) {
		double left = rho * S / (1 - rho);
		double right = ((cA * cA) + (cS * cS)) / 2;

		return left * right;
	}
}
