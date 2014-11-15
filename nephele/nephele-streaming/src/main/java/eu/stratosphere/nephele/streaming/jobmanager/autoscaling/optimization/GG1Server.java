package eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization;

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;

public class GG1Server {

	private final JobVertexID groupVertexID;

	private final double lambdaTotal;

	private final int p;

	private final double varA;

	private final double S;

	private final double varS;

	private final double cS;

	private final double fittingFactor;

	private final int lowerBoundParallelism;

	private final int upperBoundParallelism;

	public GG1Server(JobVertexID groupVertexID, int minSubtasks,
			int maxSubtasks, QosGroupEdgeSummary edgeSummary) {
		this.groupVertexID = groupVertexID;

		// measured values
		lambdaTotal = edgeSummary.getMeanEmissionRate()
				* edgeSummary.getActiveEmitterVertices();
		p = edgeSummary.getActiveConsumerVertices();
		varA = edgeSummary.getMeanConsumerVertexInterarrivalTimeVariance() / (1000*1000);
		S = edgeSummary.getMeanConsumerVertexLatency() / 1000;
		varS = edgeSummary.getMeanConsumerVertexLatencyVariance() / (1000*1000);

		// derived values
		cS = Math.sqrt(varS) / S;
		fittingFactor = edgeSummary.getTransportLatencyMean() / 1000
				/ getKLBQueueWaitUnadjusted(p);
		lowerBoundParallelism = Math.max((int) Math.ceil(lambdaTotal * S),
				minSubtasks);
		upperBoundParallelism = maxSubtasks;
	}

	public JobVertexID getGroupVertexID() {
		return groupVertexID;
	}

	private double getKingmanQueueWait(int newP, double rho, double newA,
			double cA) {
		double left = rho * S / (1 - rho);
		double right = ((cA * cA) + (cS * cS)) / 2;

		return left * right;
	}

	private double getKLBCorrectionFactor(int newP, double rho, double newA,
			double cA) {
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

	private double getKLBQueueWaitUnadjusted(int newP) {
		double rho = getMeanUtilization(newP);
		double newA = newP / lambdaTotal;
		double cA = Math.sqrt(varA) / newA;

		if (rho < 1) {
			return getKingmanQueueWait(newP, rho, newA, cA)
					* getKLBCorrectionFactor(newP, rho, newA, cA);
		} else {
			return Double.NaN;
		}
	}

	public double getKLBQueueWait(int newP) {
		return getKLBQueueWaitUnadjusted(newP) * fittingFactor;
	}

	public int getLowerBoundParallelism() {
		return lowerBoundParallelism;
	}

	public int getUpperBoundParallelism() {
		return upperBoundParallelism;
	}
	
	public int getCurrentParallelism() {
		return p;
	}
	
	public double getMeanUtilization(int newP) {
		return S * lambdaTotal / newP;
	}
}
