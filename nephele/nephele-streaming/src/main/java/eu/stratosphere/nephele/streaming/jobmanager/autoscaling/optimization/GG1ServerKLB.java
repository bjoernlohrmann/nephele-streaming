package eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization;

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;

/**
 * Implements the Kraemer-Langenbach-Belz formula for G/G/1 queueing systems, which
 * is based on the "standard" Kingman formula.
 * 
 * @author Bjoern Lohrmann
 */
public class GG1ServerKLB extends GG1ServerKingman {

	public GG1ServerKLB(JobVertexID groupVertexID, int minSubtasks,
			int maxSubtasks, QosGroupEdgeSummary edgeSummary) {
		super(groupVertexID, minSubtasks, maxSubtasks, edgeSummary);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected double getQueueWaitUnfitted(int newP, double rho) {
		return super.getQueueWaitUnfitted(newP, rho) * getKLBCorrectionFactor(rho);
	}

	private double getKLBCorrectionFactor(double rho) {
		double exponent;

		if (cA > 1) {
			exponent = (-1) * (1 - rho) * (cA * cA - 1)
					/ (cA * cA + 4 * cS * cS);
		} else {
			exponent = (-2 * (1 - rho) * (1 - cA * cA) * (1 - cA * cA))
					/ (3 * rho * (cA * cA + cS * cS));
		}

		return Math.exp(exponent);
	}
}
