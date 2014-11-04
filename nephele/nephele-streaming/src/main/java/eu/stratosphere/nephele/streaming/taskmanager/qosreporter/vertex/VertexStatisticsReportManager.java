package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import eu.stratosphere.nephele.streaming.SamplingStrategy;
import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReporterConfigCenter;
import eu.stratosphere.nephele.streaming.util.StreamUtil;

/**
 * Handles the measurement and reporting of latencies and record
 * consumption/emission rates for a particular vertex. Such a latency is defined
 * as the timespan between record receptions and emits on a particular
 * input/output gate combination of the vertex. Thus one vertex may have
 * multiple associated latencies, one for each input/output gate combination.
 * Which gate combination is measured and reported on must be configured by
 * calling
 * {@link #addReporter(int, int, eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID.Vertex, eu.stratosphere.nephele.streaming.SamplingStrategy)}
 * .
 * <p/>
 * An {@link VertexStatistics} record per configured input/output gate
 * combination will be handed to the provided {@link QosReportForwarderThread}
 * approximately once per aggregation interval (see
 * {@link QosReporterConfigCenter}). "Approximately" because if no records have
 * been received/emitted, nothing will be reported.
 * 
 * @author Bjoern Lohrmann
 */
public class VertexStatisticsReportManager {

	// private static final Log LOG =
	// LogFactory.getLog(TaskLatencyReporter.class);

	private final QosReportForwarderThread reportForwarder;
	private final AtomicReferenceArray<InputGateReceiveCounter> inputGateReceiveCounter;
	private final AtomicReferenceArray<OutputGateEmitStatistics> outputGateEmitStatistics;

	private final ConcurrentHashMap<QosReporterID, VertexQosReporter> reporters;
	private final AtomicReferenceArray<VertexQosReporter[]> reportersByInputGate;
	private final AtomicReferenceArray<VertexQosReporter[]> reportersByOutputGate;

	public VertexStatisticsReportManager(QosReportForwarderThread qosReporter,
			int noOfInputGates, int noOfOutputGates) {

		this.reportForwarder = qosReporter;

		this.inputGateReceiveCounter = new AtomicReferenceArray<InputGateReceiveCounter>(
				noOfInputGates);
		this.outputGateEmitStatistics = new AtomicReferenceArray<OutputGateEmitStatistics>(
				noOfOutputGates);

		this.reportersByInputGate = StreamUtil
				.createAtomicReferenceArrayOfEmptyArrays(
						VertexQosReporter.class, noOfInputGates);
		this.reportersByOutputGate = StreamUtil
				.createAtomicReferenceArrayOfEmptyArrays(
						VertexQosReporter.class, noOfOutputGates);

		this.reporters = new ConcurrentHashMap<QosReporterID, VertexQosReporter>();
	}

	public void recordReceived(int runtimeInputGateIndex) {
		InputGateReceiveCounter igCounter = inputGateReceiveCounter
				.get(runtimeInputGateIndex);

		if (igCounter != null) {
			igCounter.recordReceived();
		}

		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(runtimeInputGateIndex)) {
			reporter.recordReceived(runtimeInputGateIndex);
		}
	}

	public void tryingToReadRecord(int runtimeInputGateIndex) {
		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(runtimeInputGateIndex)) {
			reporter.tryingToReadRecord(runtimeInputGateIndex);
		}
	}

	public void recordEmitted(int runtimeOutputGateIndex) {
		OutputGateEmitStatistics ogStats = outputGateEmitStatistics
				.get(runtimeOutputGateIndex);
		if (ogStats != null) {
			ogStats.emitted();
		}

		for (VertexQosReporter reporter : this.reportersByOutputGate
				.get(runtimeOutputGateIndex)) {
			reporter.recordEmitted(runtimeOutputGateIndex);
		}
	}

	public boolean containsReporter(QosReporterID.Vertex reporterID) {
		return this.reporters.containsKey(reporterID);
	}

	public synchronized void addReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID,
			SamplingStrategy samplingStrategy) {

		if (this.reporters.containsKey(reporterID)) {
			return;
		}

		if (!reporterID.isDummy()) {
			inputGateReceiveCounter.compareAndSet(runtimeInputGateIndex, null,
					new InputGateReceiveCounter());
			outputGateEmitStatistics.compareAndSet(runtimeOutputGateIndex, null,
					new OutputGateEmitStatistics());

			switch (samplingStrategy) {
			case READ_WRITE:
				addReadWriteReporter(runtimeInputGateIndex,
						runtimeOutputGateIndex, reporterID);
				break;

			case READ_READ:
				addReadReadReporter(runtimeInputGateIndex,
						runtimeOutputGateIndex, reporterID);
				break;
			default:
				throw new IllegalArgumentException(
						"Unsupported sampling strategy: " + samplingStrategy);
			}

		} else if (runtimeInputGateIndex != -1) {
			inputGateReceiveCounter.compareAndSet(runtimeInputGateIndex, null,
					new InputGateReceiveCounter());
			addVertexConsumptionReporter(runtimeInputGateIndex, reporterID);

		} else if (runtimeOutputGateIndex != -1) {
			outputGateEmitStatistics.compareAndSet(runtimeOutputGateIndex, null,
					new OutputGateEmitStatistics());
			addVertexEmissionReporter(runtimeOutputGateIndex, reporterID);
		}
	}

	private void addVertexEmissionReporter(int runtimeOutputGateIndex,
			QosReporterID.Vertex reporterID) {
		VertexEmissionReporter reporter = new VertexEmissionReporter(
				reportForwarder, reporterID, runtimeOutputGateIndex,
				outputGateEmitStatistics.get(runtimeOutputGateIndex));

		addToReporterArray(reportersByOutputGate, runtimeOutputGateIndex,
				reporter);
		this.reporters.put(reporterID, reporter);
	}

	private void addVertexConsumptionReporter(int runtimeInputGateIndex,
			QosReporterID.Vertex reporterID) {
		VertexConsumptionReporter reporter = new VertexConsumptionReporter(
				reportForwarder, reporterID, runtimeInputGateIndex,
				inputGateReceiveCounter.get(runtimeInputGateIndex));

		addToReporterArray(reportersByInputGate, runtimeInputGateIndex,
				reporter);
		this.reporters.put(reporterID, reporter);
	}

	public void addReadReadReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID) {
		
		// search for ReadReadVertexQosReporterGroup
		ReadReadVertexQosReporterGroup groupReporter = null;
		for (VertexQosReporter vertexQosReporter : reportersByInputGate
				.get(runtimeInputGateIndex)) {
			if (vertexQosReporter instanceof ReadReadVertexQosReporterGroup) {
				groupReporter = (ReadReadVertexQosReporterGroup) vertexQosReporter;
				break;
			}
		}
		
		// create a ReadReadVertexQosReporterGroup if none found for the given
		// runtimeInputGateIndex
		if (groupReporter == null) {
			groupReporter = new ReadReadVertexQosReporterGroup(reportForwarder, runtimeInputGateIndex,
					inputGateReceiveCounter.get(runtimeInputGateIndex));
			// READ_READ reporters needs to keep track of all input gates
			for (int i = 0; i < this.reportersByInputGate.length(); i++) {
				addToReporterArray(reportersByInputGate, i, groupReporter);
			}
		}

		ReadReadReporter reporter = new ReadReadReporter(reportForwarder,
				reporterID, groupReporter.getReportTimer(), 
				runtimeInputGateIndex, runtimeOutputGateIndex,
				inputGateReceiveCounter.get(runtimeInputGateIndex),
				outputGateEmitStatistics.get(runtimeOutputGateIndex));

		groupReporter.addReporter(reporter);
		this.reporters.put(reporterID, reporter);
	}

	public void addReadWriteReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID) {

		VertexQosReporter reporter = new ReadWriteReporter(reportForwarder,
				reporterID, runtimeInputGateIndex, runtimeOutputGateIndex,
				inputGateReceiveCounter.get(runtimeInputGateIndex),
				outputGateEmitStatistics.get(runtimeOutputGateIndex));

		// for the READ_WRITE strategy the reporter needs to keep track of the
		// events on
		// all input gates
		for (int igIndex = 0; igIndex < this.reportersByInputGate.length(); igIndex++) {
			addToReporterArray(reportersByInputGate, igIndex, reporter);
		}
		// for the READ_WRITE strategy the reporter needs to keep track of the
		// events on
		// all output gates, too
		for (int ogIndex = 0; ogIndex < this.reportersByOutputGate.length(); ogIndex++) {
			addToReporterArray(reportersByOutputGate, ogIndex, reporter);
		}
		this.reporters.put(reporterID, reporter);
	}

	private void addToReporterArray(
			AtomicReferenceArray<VertexQosReporter[]> reporterArray,
			int gateIndex, VertexQosReporter reporter) {

		reporterArray.set(gateIndex,
				StreamUtil.appendToArrayAt(reporterArray.get(gateIndex),
						VertexQosReporter.class, reporter));
	}

	public void inputBufferConsumed(int inputGateIndex, int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {

		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(inputGateIndex)) {
			reporter.inputBufferConsumed(inputGateIndex, channelIndex,
					bufferInterarrivalTimeNanos, recordsReadFromBuffer);
		}
	}
}
