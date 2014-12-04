package eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import eu.stratosphere.nephele.jobgraph.JobVertexID;

public class Rebalancer {

	private final ArrayList<GG1Server> gg1Servers;

	private final double maxTotalQueueWait;

	private final Map<JobVertexID, Integer> rebalancedParallelism = new HashMap<JobVertexID, Integer>();

	private final HashMap<JobVertexID, Integer> scalingActions = new HashMap<JobVertexID, Integer>();
	
	private int rebalancedParallelismCost;

	private double rebalancedQueueWait;

	private class Gradient implements Comparable<Gradient> {
		final int serverIndex;
		double queueWait;
		double gradient; // = queueWait(currParallelism+1) -
							// queueWait(currParallelism)

		public Gradient(int serverIndex) {
			this.serverIndex = serverIndex;
		}

		@Override
		public int compareTo(Gradient other) {
			return Double.compare(gradient, other.gradient);
		}

		public boolean exists() {
			return !Double.isInfinite(gradient);
		}
	}

	public Rebalancer(ArrayList<GG1Server> gg1Servers,
			double maxTotalQueueWaitMillis) {
		this.gg1Servers = gg1Servers;
		this.maxTotalQueueWait = maxTotalQueueWaitMillis / 1000;
	}

	private int computeCost(int[] newP) {
		int cost = 0;
		for (int p : newP) {
			cost += p;
		}
		return cost;
	}

	public boolean computeRebalancedParallelism() {
		int[] p = fillMaximum(new int[gg1Servers.size()]);
		double pQueueWait = computeQueueWait(p);

		// ensure feasibility
		if (pQueueWait > maxTotalQueueWait) {
			setRebalancedParallelism(p);
			return false;
		}

		fillEffectiveMinimum(p);
		pQueueWait = computeQueueWait(p);

		TreeSet<Gradient> queueWaitGradients = initQueueWaitGradients(p);

		while (pQueueWait > maxTotalQueueWait) {
			Gradient lowestGrad = queueWaitGradients.pollFirst();
			GG1Server lowestGradServer = gg1Servers.get(lowestGrad.serverIndex);

			int newP = lowestGradServer
					.computeParallelismForQueueWaitThreshold(maxTotalQueueWait
							- pQueueWait + lowestGrad.queueWait);

			if (queueWaitGradients.size() > 0) {
				Gradient secLowestGrad = queueWaitGradients.first();
				newP = Math
						.min(lowestGradServer
								.computeParallelismForGradientThreshold(secLowestGrad.gradient),
								newP);
			}

			if (p[lowestGrad.serverIndex] > newP) {
				throw new RuntimeException("Stuff doesn't work.");
			}

			double newPQueueWait = lowestGradServer.getQueueWait(newP);
			p[lowestGrad.serverIndex] = newP;
			pQueueWait = pQueueWait - lowestGrad.queueWait + newPQueueWait;
			lowestGrad.queueWait = newPQueueWait;
			lowestGrad.gradient = lowestGradServer
					.computeQueueWaitGradient(newP);

			if (lowestGrad.exists()) {
				queueWaitGradients.add(lowestGrad);
			}
		}

		setRebalancedParallelism(p);
		return true;
	}

	private TreeSet<Gradient> initQueueWaitGradients(int[] newP) {
		TreeSet<Gradient> set = new TreeSet<Gradient>();

		for (int i = 0; i < newP.length; i++) {
			Gradient g = new Gradient(i);
			g.queueWait = gg1Servers.get(i).getQueueWait(newP[i]);
			g.gradient = gg1Servers.get(i).computeQueueWaitGradient(newP[i]);

			if (g.exists()) {
				set.add(g);
			}

		}
		return set;
	}

	private void setRebalancedParallelism(int[] newP) {

		for (int i = 0; i < newP.length; i++) {
			rebalancedParallelism.put(gg1Servers.get(i).getGroupVertexID(),
					newP[i]);

			int action = newP[i]
					- gg1Servers.get(i).getCurrentParallelism();
			if (action != 0) {
				scalingActions
						.put(gg1Servers.get(i).getGroupVertexID(), action);
			}
		}

		rebalancedParallelismCost = computeCost(newP);
		rebalancedQueueWait = computeQueueWait(newP);
	}

	private double computeQueueWait(int[] newP) {
		double totalQueueWait = 0;
		for (int i = 0; i < newP.length; i++) {
			totalQueueWait += gg1Servers.get(i).getQueueWait(newP[i]);
		}

		return totalQueueWait;
	}

	private int[] fillMaximum(int[] newP) {
		for (int i = 0; i < gg1Servers.size(); i++) {
			newP[i] = gg1Servers.get(i).getUpperBoundParallelism();
		}
		return newP;
	}

	private int[] fillEffectiveMinimum(int[] newP) {
		for (int i = 0; i < gg1Servers.size(); i++) {
			newP[i] = gg1Servers.get(i).getLowerBoundParallelism();
		}

		return newP;
	}

	public Map<JobVertexID, Integer> getRebalancedParallelism() {
		return rebalancedParallelism;
	}

	public Map<JobVertexID, Integer> getScalingActions() {
		return scalingActions;
	}

	public int getRebalancedParallelismCost() {
		return rebalancedParallelismCost;
	}

	public double getMaxTotalQueueWait() {
		return maxTotalQueueWait;
	}

	public double getRebalancedQueueWait() {
		return rebalancedQueueWait;
	}
}
