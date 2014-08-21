package eu.stratosphere.nephele.streaming.message;

public class CpuLoadClassifier {

	public static final double MEDIUM_THRESHOLD_PERCENT = 60.0;

	public static final double HIGH_THRESHOLD_PERCENT = 85.0;

	public enum CpuLoad {
		LOW, MEDIUM, HIGH;
	}

	/**
	 * Classifies the given CPU utilization.
	 * 
	 * @param cpuUtilizationPercent
	 *            How much percent of one CPU core's available time a task's
	 *            main thread and its associated user threads consume. Example:
	 *            50 means it uses half a core's worth of CPU time. 200 means it
	 *            uses two core's worth of CPU time (this can happen if the
	 *            vertex's main thread spawns several user threads)
	 */
	public static CpuLoad fromCpuUtilization(double cpuUtilizationPercent) {
		if (cpuUtilizationPercent < MEDIUM_THRESHOLD_PERCENT) {
			return CpuLoad.LOW;
		} else if (cpuUtilizationPercent >= MEDIUM_THRESHOLD_PERCENT
				&& cpuUtilizationPercent < HIGH_THRESHOLD_PERCENT) {
			return CpuLoad.MEDIUM;
		} else {
			return CpuLoad.HIGH;
		}
	}
}
