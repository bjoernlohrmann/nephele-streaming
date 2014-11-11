package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.BernoulliSampler;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.Sample;

/**
 * Provides read-write latency measurements for a specific igX-ogY combination.
 * Read-write means that the elapsed time between recordReceived(igX) and
 * recordEmitted(ogY) (both igX and ogY are fixed) is measured (randomly sampled
 * actually).
 * 
 * 
 * @author Ilya Verbitskiy, Bjoern Lohrmann
 * 
 */
public class ReadWriteReporter extends AbstractVertexQosReporter {

	/**
	 * Samples the elapsed time between a read on the input gate identified
	 * {@link #runtimeInputGateIndex} and a write on the output gate indentified by
	 * {@link #runtimeOutputGateIndex} 
	 * Elapsed time is sampled in microseconds.
	 */
	private final BernoulliSampler vertexLatencySampler;

	private boolean retrySample;
	
	private long lastSampleReadTime;

	public ReadWriteReporter(QosReportForwarderThread reportForwarder,
			QosReporterID.Vertex reporterID, int runtimeInputGateIndex,
			int runtimeOutputGateIndex, InputGateReceiveCounter igReceiveCounter,
			OutputGateEmitStatistics emitCounter) {

		super(reportForwarder, reporterID, 
				new ReportTimer(reportForwarder.getConfigCenter().getAggregationInterval()),
				runtimeInputGateIndex, runtimeOutputGateIndex, igReceiveCounter, emitCounter);

		this.vertexLatencySampler = new BernoulliSampler(reportForwarder
				.getConfigCenter().getSamplingProbability() / 100.0);

		this.lastSampleReadTime = -1;
		this.retrySample = false;
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {

		boolean ongoingSample = (lastSampleReadTime != -1);
		boolean correctGate = runtimeInputGateIndex == getRuntimeInputGateIndex();

		if (correctGate) {
			if ((!ongoingSample && (retrySample || vertexLatencySampler
					.shouldTakeSamplePoint())) || ongoingSample) {
				// we either have no ongoing sample but want to start a new one,
				// or we have one which needs to be restarted.
				beginSample();
			}
		} else {
			// we may or may not have an ongoing sample which needs to be
			// discarded.
			discardSampleAndRetryLater();
		}
	}

	public void beginSample() {
		lastSampleReadTime = System.nanoTime();
		retrySample = false;
	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {
		if (lastSampleReadTime != -1) {
			// Retry, because sample is spoiled (we were waiting for a
			// recordEmitted()).
			discardSampleAndRetryLater();
		}
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		if (runtimeOutputGateIndex == getRuntimeOutputGateIndex()) {
			if (lastSampleReadTime != -1) {
				// yay, we have a sample!
				takeSample();
			}
			sendReportIfDue();
		} else {
			// we may or may not have an ongoing sample which needs to be
			// discarded.
			discardSampleAndRetryLater();
		}
	}

	private void sendReportIfDue() {
		if (vertexLatencySampler.hasSample() && canSendReport()) {

			long now = System.currentTimeMillis();
			
			// draw sample and rescale from micros to millis
			Sample vertexLatencySample = vertexLatencySampler.drawSampleAndReset(now).rescale(0.001);
			sendReport(now, vertexLatencySample);
		}
	}
	
	public void takeSample() {
		vertexLatencySampler
				.addSamplePoint((System.nanoTime() - lastSampleReadTime) / 1000);
		lastSampleReadTime = -1;
		retrySample = false;
	}

	public void discardSampleAndRetryLater() {
		lastSampleReadTime = -1;
		retrySample = true;
	}
}
