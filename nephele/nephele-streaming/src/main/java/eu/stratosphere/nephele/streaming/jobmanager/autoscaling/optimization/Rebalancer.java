package eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.jobgraph.JobVertexID;

public class Rebalancer {

	private final ArrayList<GG1Server> gg1Servers;

	private final double maxTotalQueueWait;

	private Map<JobVertexID, Integer> rebalancedParallelism;

	private int rebalancedParallelismCost;

	private HashMap<JobVertexID, Integer> scalingActions;

	public Rebalancer(ArrayList<GG1Server> gg1Servers, double maxTotalQueueWaitMillis) {
		this.gg1Servers = gg1Servers;
		this.maxTotalQueueWait = maxTotalQueueWaitMillis/1000;
	}

	private int minimizeCost(int[] newP) {

		double currPQueueWait = computeQueueWait(newP);

		while (true) {
			int smallestGradientIndex = -1;
			double smallestGradient = Double.MAX_VALUE;

			for (int i = 0; i < newP.length; i++) {
				GG1Server server = gg1Servers.get(i);

				if (server.getLowerBoundParallelism() < newP[i]) {
					double gradient = server.getKLBQueueWait(newP[i] - 1)
							- server.getKLBQueueWait(newP[i]);
					if (gradient < smallestGradient) {
						smallestGradientIndex = i;
						smallestGradient = gradient;
					}
				}
			}

			if (smallestGradientIndex != -1
					&& currPQueueWait + smallestGradient <= maxTotalQueueWait) {
				newP[smallestGradientIndex]--;
				currPQueueWait += smallestGradient;
			} else {
				break;
			}
		}

		return computeCost(newP);
	}

	private int computeCost(int[] newP) {
		int cost = 0;
		for (int p : newP) {
			cost += p;
		}
		return cost;
	}

	public boolean computeRebalancedParallelism() {
		int[] lowestCostP = null;
		int lowestCost = Integer.MAX_VALUE;
		boolean lowestCostPFound = false;

		for (int i = 0; i < 10; i++) {
			int[] newP = getRandomHighCostParallelism();

			if (newP == null) {
				lowestCostP = new int[gg1Servers.size()];
				fillMaximum(lowestCostP);
				lowestCost = computeCost(lowestCostP);
				break;
			}

			int newPCost = minimizeCost(newP);
			if (newPCost < lowestCost) {
				lowestCostPFound = true;
				lowestCostP = newP;
				lowestCost = newPCost;
			}
		}

		setRebalancedParallelismWithCost(lowestCostP, lowestCost);
		return lowestCostPFound;
	}

	private void setRebalancedParallelismWithCost(int[] lowestCostP, int lowestCost) {
		rebalancedParallelism = new HashMap<JobVertexID, Integer>();
		scalingActions = new HashMap<JobVertexID, Integer>();
		
		for (int i = 0; i < lowestCostP.length; i++) {
			rebalancedParallelism.put(gg1Servers.get(i).getGroupVertexID(), lowestCostP[i]);
			
			int action = lowestCostP[i]
					- gg1Servers.get(i).getCurrentParallelism();
			if (action != 0) {
				scalingActions.put(gg1Servers.get(i).getGroupVertexID(), action);
			}
		}
		
		rebalancedParallelismCost = lowestCost;
	}

	private double computeQueueWait(int[] newP) {
		double totalQueueWait = 0;
		for (int i = 0; i < newP.length; i++) {
			totalQueueWait += gg1Servers.get(i).getKLBQueueWait(newP[i]);
		}

		return totalQueueWait;
	}

	/**
	 * Returns a new set of randomized parallism parameters with queueWait <
	 * maxTotalQueueWait.
	 * 
	 * @return new set of randomized parallism parameters, or null if none
	 *         exist.
	 */
	private int[] getRandomHighCostParallelism() {
		int[] newP = new int[gg1Servers.size()];

		int attempts = 0;
		while (attempts < 10) {
			fillRandom(newP);

			if (computeQueueWait(newP) < maxTotalQueueWait) {
				return newP;
			}

			attempts++;
		}

		fillMaximum(newP);
		if (computeQueueWait(newP) < maxTotalQueueWait) {
			return newP;
		}

		return null;
	}

	private void fillMaximum(int[] newP) {
		for (int i = 0; i < gg1Servers.size(); i++) {
			newP[i] = gg1Servers.get(i).getUpperBoundParallelism();
		}
	}

	private void fillRandom(int[] newP) {
		for (int i = 0; i < gg1Servers.size(); i++) {
			int currP = gg1Servers.get(i).getCurrentParallelism();
			int maxP = gg1Servers.get(i).getUpperBoundParallelism();
			newP[i] = (int) Math.ceil(currP + (maxP - currP) * Math.random());
		}
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
}
