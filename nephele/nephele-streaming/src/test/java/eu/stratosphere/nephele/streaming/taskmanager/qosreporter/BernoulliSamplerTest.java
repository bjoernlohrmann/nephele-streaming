package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.BernoulliSampler;

public class BernoulliSamplerTest {

	private BernoulliSampler bernoulliSampler;

	@Before
	public void setup() {
		this.bernoulliSampler = new BernoulliSampler(0.5);
	}

	@Test
	public void testShouldSampleFirstCallIsTrue() {
		for (int i = 0; i < 100; i++) {
			assertTrue(bernoulliSampler.shouldTakeSamplePoint());
			bernoulliSampler.reset(0);
		}
	}

	/**
	 * This test whether shouldSample() sometimes returns true and sometimes
	 * returns false. There is a non-zero (however negligible) probability that
	 * the test fails although the shouldSample() implementation is correct. The
	 * probability of this happening is however 0.5^(200 -1), in other words
	 * "this just does not happen".
	 */
	@Test
	public void testShouldSampleReturnsFalse() {
		assertTrue(bernoulliSampler.shouldTakeSamplePoint());

		boolean gotShouldNotSample = false;
		boolean gotShouldSample = false;
		for (int i = 0; i < 200; i++) {
			if (bernoulliSampler.shouldTakeSamplePoint()) {
				gotShouldSample = true;
			} else {
				gotShouldNotSample = true;
			}
		}

		assertTrue(gotShouldNotSample);
		assertTrue(gotShouldSample);
	}

	@Test
	public void testMeanAndVarianceUniform() {
		bernoulliSampler.addSamplePoint(10);
		bernoulliSampler.addSamplePoint(10);
		bernoulliSampler.addSamplePoint(10);

		assertTrue(bernoulliSampler.getMean() == 10);
		assertTrue(bernoulliSampler.getVariance() == 0);
	}

	@Test
	public void testMeanAndVariancePoisson() {
		// poisson distributed values (lambda = 100)
		long[] values = { 102, 102, 86, 102, 80, 127, 81, 82, 90, 100 };

		for (double value : values) {
			bernoulliSampler.addSamplePoint(value);
		}
		assertTrue(Math.abs(bernoulliSampler.getMean() - 95.2) < 0.000001);
		assertTrue((bernoulliSampler.getVariance() - 210.1778) < 0.000001);
	}

	@Test
	public void testMeanAndVarianceExponential() {
		// exponentially distributed values (lambda = 100)
		double[] values = { 0.013794656, 0.005578824, 0.034395475, 0.013030417,
				0.029795851, 0.001790420, 0.020289172, 0.004736554,
				0.027379384, 0.001943183 };

		for (double value : values) {
			bernoulliSampler.addSamplePoint(value);
		}
		assertTrue(Math.abs(bernoulliSampler.getMean() - 0.01527339) / 0.01527339 < 0.000001);
		assertTrue((bernoulliSampler.getVariance() - 0.0001466646) / 0.0001466646 < 0.000001);
	}
	
	@Test
	public void testMeanAndLargeVariance() {
		// high variance data with no special distribution 
		double[] values = { 1, 500000, 3, 1, 2, 4, 100000, 1, 2, 3, 4, 500000 };
		for (double value : values) {
			bernoulliSampler.addSamplePoint(value);
		}
		assertTrue(Math.abs(bernoulliSampler.getMean() - 91668.42) /  91668.42 < 0.000001);
		assertTrue((bernoulliSampler.getVariance() - 37196619699d) < 0.2);		
	}
}
