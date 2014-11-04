package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import eu.stratosphere.nephele.streaming.util.StreamUtil;

public class ReportTimer {

	private final ScheduledFuture<?> timerFuture;
	private volatile boolean reportIsDue;
	private long timeOfLastReport;

	public ReportTimer(long reportingIntervalMillis) {
		this.reportIsDue = false;
		this.timeOfLastReport = System.currentTimeMillis();

		Runnable timerRunnable = new Runnable() {
			@Override
			public void run() {
				reportIsDue = true;
			}
		};

		timerFuture = StreamUtil.scheduledAtFixedRate(timerRunnable,
				reportingIntervalMillis, reportingIntervalMillis,
				TimeUnit.MILLISECONDS);
	}

	public boolean reportIsDue() {
		return reportIsDue;
	}

	public void shutdown() {
		timerFuture.cancel(true);
	}

	public long getTimeOfLastReport() {
		return timeOfLastReport;
	}

	public void reset(long now) {
		timeOfLastReport = now;
		reportIsDue = false;
	}
}
