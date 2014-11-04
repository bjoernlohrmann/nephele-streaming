package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.BernoulliSampler;

public class QosStatisticTest {

	private QosStatistic qosStatistic;
	
	private QosStatistic qosStatisticWithVar;

	@Before
	public void setup() {
		this.qosStatistic = new QosStatistic(7);
		this.qosStatisticWithVar = new QosStatistic(7, true);
	}

	@Test
	public void testValueSorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 1));
		this.qosStatistic.addValue(this.createQosValue(2, 3));
		this.qosStatistic.addValue(this.createQosValue(3, 7));
		this.qosStatistic.addValue(this.createQosValue(4, 8));
		this.qosStatistic.addValue(this.createQosValue(5, 21));
		this.qosStatistic.addValue(this.createQosValue(6, 35));
		this.qosStatistic.addValue(this.createQosValue(7, 41));

		assertTrue(this.qosStatistic.getMean() - 16.5714 < 0.0001);
		assertFalse(this.qosStatistic.hasVariance());
	}

	private QosValue createQosValue(long timestamp, double mean) {
		return new QosValue(mean, timestamp);
	}
	
	private QosValue createQosValue(long timestamp, double mean, double variance, int weight) {
		return new QosValue(mean, variance, weight, timestamp);
	}

	@Test
	public void testAddValueUnsorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 7));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		this.qosStatistic.addValue(this.createQosValue(3, 13));
		this.qosStatistic.addValue(this.createQosValue(4, 1));
		this.qosStatistic.addValue(this.createQosValue(5, 5));
		this.qosStatistic.addValue(this.createQosValue(6, 7.5));
		this.qosStatistic.addValue(this.createQosValue(7, 8));

		assertTrue(this.qosStatistic.getMean() - 8.0714 < 0.0001);
		assertFalse(this.qosStatistic.hasVariance());
	}

	@Test
	public void testAddValueReverseSorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 18));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		this.qosStatistic.addValue(this.createQosValue(3, 13));
		this.qosStatistic.addValue(this.createQosValue(4, 10));
		this.qosStatistic.addValue(this.createQosValue(5, 9));
		this.qosStatistic.addValue(this.createQosValue(6, 8));
		this.qosStatistic.addValue(this.createQosValue(7, 7));

		assertTrue(this.qosStatistic.getMean() - 11.4285 < 0.0001);
		assertFalse(this.qosStatistic.hasVariance());
	}

	@Test
	public void testAddValueOverfullUnsorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 7));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		this.qosStatistic.addValue(this.createQosValue(3, 13));
		this.qosStatistic.addValue(this.createQosValue(4, 1));
		this.qosStatistic.addValue(this.createQosValue(5, 7.5));
		this.qosStatistic.addValue(this.createQosValue(6, 5));
		this.qosStatistic.addValue(this.createQosValue(7, 18));
		this.qosStatistic.addValue(this.createQosValue(8, 13));
		this.qosStatistic.addValue(this.createQosValue(9, 10));
		this.qosStatistic.addValue(this.createQosValue(10, 8));

		assertTrue(this.qosStatistic.getMean() - 8.9285 < 0.0001);
		assertFalse(this.qosStatistic.hasVariance());
	}

	@Test
	public void testAddValueOverfullSorted() {
		this.qosStatistic.addValue(this.createQosValue(1, 1));
		this.qosStatistic.addValue(this.createQosValue(2, 2));
		this.qosStatistic.addValue(this.createQosValue(3, 3));
		this.qosStatistic.addValue(this.createQosValue(4, 4));
		this.qosStatistic.addValue(this.createQosValue(5, 5));
		this.qosStatistic.addValue(this.createQosValue(6, 6));
		this.qosStatistic.addValue(this.createQosValue(7, 7));
		this.qosStatistic.addValue(this.createQosValue(8, 8));
		this.qosStatistic.addValue(this.createQosValue(9, 9));
		this.qosStatistic.addValue(this.createQosValue(10, 10));

		assertTrue(this.qosStatistic.getMean() == 7);
		assertFalse(this.qosStatistic.hasVariance());
	}

	@Test
	public void testGetArithmeticMeanUnderfull() {
		this.qosStatistic.addValue(this.createQosValue(1, 18));
		this.qosStatistic.addValue(this.createQosValue(2, 15));
		assertTrue(this.qosStatistic.getMean() == 16.5);

		this.qosStatistic.addValue(this.createQosValue(3, 17));
		assertTrue(this.qosStatistic.getMean() - 16.6666 < 0.0001);
		assertFalse(this.qosStatistic.hasVariance());
	}
	
	@Test
	public void testGetVarianceWithZeroSubvariances() {
		this.qosStatisticWithVar.addValue(this.createQosValue(1, 18, 0, 2));
		this.qosStatisticWithVar.addValue(this.createQosValue(2, 15, 0, 3));
		this.qosStatisticWithVar.addValue(this.createQosValue(3, 35, 0, 4));
		
		assertTrue(this.qosStatisticWithVar.getMean() - 24.55556 < 0.0001);
		assertTrue(this.qosStatisticWithVar.hasVariance());
		assertTrue(this.qosStatisticWithVar.getVariance() - 99.52778 < 0.0001);		
	}
	
	@Test
	public void testGetVarianceWithExponentiallyDistributedValues() {	
		// 0.013794656, 0.005578824
		this.qosStatisticWithVar.addValue(this.createQosValue(1, 0.01792299, 0.0002203822, 2));
		
		// 0.034395475, 0.013030417, 0.029795851, 0.001790420, 0.020289172
		this.qosStatisticWithVar.addValue(this.createQosValue(2, 0.01986027, 0.0001708346, 5));
		
		// 0.004736554, 0.027379384, 0.001943183
		this.qosStatisticWithVar.addValue(this.createQosValue(3, 0.01135304, 0.0001945835, 3));
		
		assertTrue(this.qosStatisticWithVar.getMean() - 0.01527339 < 0.000001);
		assertTrue(this.qosStatisticWithVar.hasVariance());
		// unfortunately combined variance computation is not very numerically stable :-/
		assertTrue(this.qosStatisticWithVar.getVariance() - 0.0001466646 < 0.0001);		
	}
	
	@Test
	public void testGetVarianceWithHighVarianceValues() {
		double[] data = {1, 500000, 3, 1, 2, 4, 100000, 1, 2, 3, 4, 500000};
		double[] subset1 = Arrays.copyOfRange(data, 0, 4);
		double[] subset2 = Arrays.copyOfRange(data, 4, 10);
		double[] subset3 = Arrays.copyOfRange(data, 10, 12);

		this.qosStatisticWithVar.addValue(createQosValueFromDataset(1L, subset1));
		this.qosStatisticWithVar.addValue(createQosValueFromDataset(2L, subset2));
		this.qosStatisticWithVar.addValue(createQosValueFromDataset(3L, subset3));
		
		QosValue total = createQosValueFromDataset(1L, data);
		assertTrue(Math.abs(this.qosStatisticWithVar.getMean() - total.getMean()) / total.getMean() < 0.00000000001);
		assertTrue(this.qosStatisticWithVar.hasVariance());
		assertTrue(Math.abs(this.qosStatisticWithVar.getVariance() - total.getVariance()) / total.getVariance() < 0.00000000001);		
	}

	private QosValue createQosValueFromDataset(long timestamp, double[] dataset) {
		BernoulliSampler sampler = new BernoulliSampler(0.1);
		for(double val : dataset) {
			sampler.addSamplePoint(val);
		}
		return new QosValue(sampler.getMean(), sampler.getVariance(), dataset.length, timestamp);
	}
	
	@Test
	public void testOverfullWithHighVarianceValues() {
		double[] data = {1, 500000, 3, 1, 2, 4, 100000, 1, 2, 3, 4, 500000};
		
		double[][] subsets = {Arrays.copyOfRange(data, 0, 4), Arrays.copyOfRange(data, 4, 10), Arrays.copyOfRange(data, 10, 12)};		
		
		for (int i = 0; i < 9; i++) {
			this.qosStatisticWithVar.addValue(createQosValueFromDataset(i, subsets[i % subsets.length]));	
		}

		double[] dataInStatisticWindow = {};
		for (int i = 8; i >=2; i--) {
			dataInStatisticWindow = ArrayUtils.addAll(dataInStatisticWindow, subsets[i % subsets.length]);
		}
		
		QosValue total = createQosValueFromDataset(1L, dataInStatisticWindow);
		assertTrue(Math.abs(this.qosStatisticWithVar.getMean() - total.getMean()) / total.getMean() < 0.00000000001);
		assertTrue(this.qosStatisticWithVar.hasVariance());
		assertTrue(Math.abs(this.qosStatisticWithVar.getVariance() - total.getVariance()) / total.getVariance() < 0.00000000001);		
	}	
}
