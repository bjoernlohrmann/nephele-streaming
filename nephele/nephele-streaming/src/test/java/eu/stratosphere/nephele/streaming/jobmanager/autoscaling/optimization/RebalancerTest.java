package eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;

public class RebalancerTest {
	
	@Test
	public void testRebalanceWith1Edge() {
		ArrayList<GG1Server> servers = new ArrayList<GG1Server>(Arrays.asList(
				(GG1Server) GG1ServerTest.createKingmanEdge1()));
		
		Rebalancer reb = new Rebalancer(servers, 4);
		assertTrue(reb.computeRebalancedParallelism());
		assertSolutionIsCompleteAndValid(servers, reb, true);
		assertSolutionIsOptimal(servers, reb);
	}	
	
	@Test
	public void testRebalanceWith2Edges() {
		ArrayList<GG1Server> servers = new ArrayList<GG1Server>(Arrays.asList(
				(GG1Server) GG1ServerTest.createKingmanEdge1(),
				GG1ServerTest.createKingmanEdge2()));
		
		Rebalancer reb = new Rebalancer(servers, 10);
		assertTrue(reb.computeRebalancedParallelism());
		assertSolutionIsCompleteAndValid(servers, reb, true);
		assertSolutionIsOptimal(servers, reb);
	}
	
	@Test
	public void testRebalanceWith3Edges() {
		ArrayList<GG1Server> servers = new ArrayList<GG1Server>(Arrays.asList(
				(GG1Server) GG1ServerTest.createKingmanEdge1(),
				GG1ServerTest.createKingmanEdge2(),
				GG1ServerTest.createKingmanEdge3()));
		
		Rebalancer reb = new Rebalancer(servers, 15);
		assertTrue(reb.computeRebalancedParallelism());
		assertSolutionIsCompleteAndValid(servers, reb, true);
		assertSolutionIsOptimal(servers, reb);
	}
	
	private void assertSolutionIsCompleteAndValid(ArrayList<GG1Server> servers,
			Rebalancer reb, boolean success) {
		
		Map<JobVertexID, Integer> result = reb.getRebalancedParallelism();
		assertTrue(result.size() == servers.size());
		
		double latencySum = 0;
		
		for (GG1Server server : servers) {
			JobVertexID id = server.getGroupVertexID();
			assertTrue(server.getLowerBoundParallelism() <= result.get(id));
			assertTrue(result.get(id) <= server.getUpperBoundParallelism());
			assertTrue(server.getMeanUtilization(result.get(id)) < 1);
			latencySum += server.getQueueWait(result.get(id));
		}
		
		if(success) {
			assertTrue(latencySum <= reb.getMaxTotalQueueWait());
		} else {
			assertTrue(latencySum > reb.getMaxTotalQueueWait());
		}
	}
	
	private void assertSolutionIsOptimal(ArrayList<GG1Server> servers,
			Rebalancer reb) {

		int optimalCost = Integer.MAX_VALUE;

		int[] p = new int[servers.size()];
		for (int i = 0; i < p.length; i++) {
			p[i] = servers.get(i).getLowerBoundParallelism();
		}

		while (true) {
			double latencySum = 0;
			int cost = 0;
			for (int i = 0; i < p.length; i++) {
				latencySum += servers.get(i).getQueueWait(p[i]);
				cost += p[i];
			}

			if (latencySum <= reb.getMaxTotalQueueWait() && cost < optimalCost) {
				optimalCost = cost;
			}

			boolean changed = false;
			for (int i = p.length - 1; i >= 0; i--) {
				if (p[i] < servers.get(i).getUpperBoundParallelism()) {
					p[i]++;
					changed = true;
					for (int j = i + 1; j < p.length; j++) {
						p[j] = servers.get(j).getLowerBoundParallelism();
					}
					break;
				}
			}

			if (!changed) {
				break;
			}
		}

		assertEquals(optimalCost, reb.getRebalancedParallelismCost());
	}

	@Test
	public void testRebalanceWith10Edges() {
		Random rnd = new Random(234627834);
		
		for (int i = 0; i < 10; i++) {
			ArrayList<GG1Server> servers = create10RandomizedServers(rnd);
			Rebalancer reb = new Rebalancer(servers, 10);
			assertTrue(reb.computeRebalancedParallelism());
			assertSolutionIsCompleteAndValid(servers, reb, true);
		}
	}
	
	private ArrayList<GG1Server> create10RandomizedServers(Random rnd) {
		
		ArrayList<GG1Server> servers = new ArrayList<GG1Server>();
		
		for(int i=0; i< 9; i++) {
			QosGroupEdgeSummary edgeSum = GG1ServerTest.getEdge1();
			
			double serverUtil = 0.2 + 0.7 * rnd.nextDouble();
			
			double lambdaTotal = edgeSum.getMeanEmissionRate() * edgeSum.getActiveEmitterVertices() * rnd.nextDouble();
			double s = edgeSum.getMeanConsumerVertexLatency() * 2 * rnd.nextDouble();
			double varS = edgeSum.getMeanConsumerVertexLatencyCV() * 2 * rnd.nextDouble();
			double varA = edgeSum.getMeanConsumerVertexInterarrivalTimeCV() * 2 * rnd.nextDouble();
			int maxP = 1000;
			int p = (int) Math.ceil(lambdaTotal * (s / 1000) / serverUtil);
			double queueWait = (1 + (9*rnd.nextDouble()));
			
			edgeSum.setMeanEmissionRate(lambdaTotal / edgeSum.getActiveEmitterVertices());
			edgeSum.setMeanConsumerVertexLatency(s);
			edgeSum.setMeanConsumerVertexLatencyCV(Math.sqrt(varS) / s);
			edgeSum.setMeanConsumerVertexInterarrivalTimeCV(Math.sqrt(varA)
					/ (p / lambdaTotal));
			edgeSum.setActiveConsumerVertices(p);
			edgeSum.setTransportLatencyMean(queueWait);
			
			servers.add(new GG1ServerKingman(new JobVertexID(), 1, maxP, edgeSum));
		}
		
		servers.add(new GG1ServerKingman(new JobVertexID(), 1, 100, GG1ServerTest.getEdge2()));
		return servers;
	}
	
	@Test
	public void testImpossibleRebalance() {
		ArrayList<GG1Server> servers = new ArrayList<GG1Server>(Arrays.asList(
				(GG1Server) GG1ServerTest.createKingmanEdge1(),
				GG1ServerTest.createKingmanEdge2(),
				GG1ServerTest.createKingmanEdge3()));
		
		Rebalancer reb = new Rebalancer(servers, 1);
		assertFalse(reb.computeRebalancedParallelism());
		assertSolutionIsCompleteAndValid(servers, reb, false);
		
		Map<JobVertexID, Integer> result = reb.getRebalancedParallelism();
		int maxCost = 0;
		assertTrue(result.size() == servers.size());
		for (GG1Server server : servers) {
			JobVertexID id = server.getGroupVertexID();
			assertTrue(result.get(id) == server.getUpperBoundParallelism());
			maxCost += server.getUpperBoundParallelism();
		}
		assertEquals(maxCost, reb.getRebalancedParallelismCost());
	}
}
