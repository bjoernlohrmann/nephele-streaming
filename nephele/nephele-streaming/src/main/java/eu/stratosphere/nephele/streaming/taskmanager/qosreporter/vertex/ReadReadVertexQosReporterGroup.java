package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.BernoulliSampler;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.Sample;

/**
 * Contains a group of {@link ReadReadReporter} to do read-read latency
 * measurements on a specific input gate. Read-read means, that the elapsed
 * time between recordReceived(igX) and the immediate next tryingToRead(ig*)
 * (igX is fixed, ig* is arbitrary) is measured (randomly sampled, actually). 
 * 
 * Since any obtained read-read measurement is usable by all {@link ReadReadReporter} 
 * in the group, obtained samples are shared.
 * 
 * @author Ilya Verbitskiy, Bjoern Lohrmann
 */
public class ReadReadVertexQosReporterGroup implements VertexQosReporter {

	private final List<ReadReadReporter> reporters = new ArrayList<ReadReadReporter>();
	
	private final ReportTimer reportTimer;
	
	private final InputGateReceiveCounter igReceiveCounter;
	private long igReceiveCounterAtLastReport;
	
	private final InputGateInterarrivalTimeSampler igInterarrivalTimeSampler;

	private final int inputGateIndex;

	/**
	 * Samples the elapsed time between a read on the input gate identified
	 * {@link #inputGateIndex} and the next read on any other input gate.
	 * Elapsed time is sampled in microseconds.
	 */
	private final BernoulliSampler vertexLatencySampler;

	private long lastSampleReadTime;

	public ReadReadVertexQosReporterGroup(
			QosReportForwarderThread reportForwarder, int inputGateIndex,
			InputGateReceiveCounter igReceiveCounter) {

		this.inputGateIndex = inputGateIndex;

		this.vertexLatencySampler = new BernoulliSampler(reportForwarder
				.getConfigCenter().getSamplingProbability() / 100.0);
		
		this.igReceiveCounter = igReceiveCounter;
		this.igReceiveCounterAtLastReport = igReceiveCounter.getRecordsReceived();
		
		this.igInterarrivalTimeSampler = new InputGateInterarrivalTimeSampler(reportForwarder
				.getConfigCenter().getSamplingProbability() / 100.0);
		
		this.reportTimer = new ReportTimer(reportForwarder.getConfigCenter()
				.getAggregationInterval());
		
		this.lastSampleReadTime = -1;
	}

	public void addReporter(ReadReadReporter reporter) {
		reporters.add(reporter);
	}

	protected void sendReportIfDue() {
		if (reportTimer.reportIsDue() 
				&& vertexLatencySampler.hasSample()
				&& igInterarrivalTimeSampler.hasSample()) {

			long now = System.currentTimeMillis();
			
			// draw sample and rescale from micros to millis
			Sample vertexLatency = vertexLatencySampler.drawSampleAndReset(now).rescale(0.001);
			Sample interarrivalTime = igInterarrivalTimeSampler.drawSampleAndReset(now).rescale(0.001);
			
			double recordsConsumedPerSec = getRecordsConsumedPerSec((now - reportTimer.getTimeOfLastReport()) / 1000.0);
			
			for (ReadReadReporter reporter : reporters) {
				reporter.sendReport(now, vertexLatency, interarrivalTime, recordsConsumedPerSec);
			}

			reportTimer.reset(now);
		}
	}
	
	private double getRecordsConsumedPerSec(double secsPassed) {
		double recordsConsumedPerSec = -1;
		if (igReceiveCounter != null) {
			recordsConsumedPerSec = (igReceiveCounter.getRecordsReceived() - igReceiveCounterAtLastReport)
					/ secsPassed;
			igReceiveCounterAtLastReport = igReceiveCounter.getRecordsReceived();
		}
		return recordsConsumedPerSec;
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {
		if (runtimeInputGateIndex == this.inputGateIndex) {
			if (vertexLatencySampler.shouldTakeSamplePoint()) {
				lastSampleReadTime = System.nanoTime();
			}
			sendReportIfDue();
		}
	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {
		if (lastSampleReadTime != -1) {
			// if lastSampleReadTime is set then we should sample
			vertexLatencySampler.addSamplePoint((System.nanoTime() - lastSampleReadTime) / 1000);
			lastSampleReadTime = -1;
		}
	}
	
	@Override
	public void inputBufferConsumed(int inputGateIndex, int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
		
		igInterarrivalTimeSampler.inputBufferConsumed(channelIndex, bufferInterarrivalTimeNanos, recordsReadFromBuffer);
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		throw new RuntimeException("Method should never be invoked. This is bug.");
	}

	@Override
	public int getRuntimeInputGateIndex() {
		return inputGateIndex;
	}

	@Override
	public int getRuntimeOutputGateIndex() {
		return -1;
	}

	public ReportTimer getReportTimer() {
		return this.reportTimer;
	}
}
