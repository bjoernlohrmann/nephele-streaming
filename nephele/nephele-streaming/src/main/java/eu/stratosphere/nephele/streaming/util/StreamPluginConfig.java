package eu.stratosphere.nephele.streaming.util;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.plugins.PluginManager;

public class StreamPluginConfig {

	/**
	 * Name of configuration entry which defines the interval in which received
	 * tags shall be aggregated and sent to the job manager plugin component.
	 */
	public static final String AGGREGATION_INTERVAL_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosreporter.aggregationinterval");

	/**
	 * The default aggregation interval.
	 */
	public static final long DEFAULT_AGGREGATION_INTERVAL = 1000;

	/**
	 * Name of the configuration entry which defines the interval in which
	 * records shall be tagged.
	 */
	public static final String SAMPLING_PROBABILITY_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosreporter.samplingprobability");

	/**
	 * The default sampling probability in percent.
	 */
	public static final int DEFAULT_SAMPLING_PROBABILITY = 10;

	/**
	 * Name of the configuration entry which defines the QoS constraint log file
	 * location.
	 */
	public static final String LOGFILE_PATTERN_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.logging.qos_statistics_filepattern");

	public static final String DEFAULT_LOGFILE_PATTERN = "/tmp/qos_statistics_%s";

	/**
	 * Name of the configuration entry which defines the time interval for QoS
	 * driven adjustments.
	 */
	public static final String QOSMANAGER_ADJUSTMENTINTERVAL_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.adjustmentinterval");

	public static final long DEFAULT_ADJUSTMENTINTERVAL = 5000;

	public static long getAggregationIntervalMillis() {
		return GlobalConfiguration.getLong(AGGREGATION_INTERVAL_KEY,
				DEFAULT_AGGREGATION_INTERVAL);
	}

	public static long getAdjustmentIntervalMillis() {
		return GlobalConfiguration.getLong(QOSMANAGER_ADJUSTMENTINTERVAL_KEY,
				DEFAULT_ADJUSTMENTINTERVAL);
	}

	public static int getSamplingProbabilityPercent() {
		return GlobalConfiguration.getInteger(SAMPLING_PROBABILITY_KEY,
				DEFAULT_SAMPLING_PROBABILITY);
	}

	public static String getQosStatisticsLogfilePattern() {
		return GlobalConfiguration.getString(LOGFILE_PATTERN_KEY,
				DEFAULT_LOGFILE_PATTERN);
	}

	public static int computeQosStatisticWindowSize() {
		return (int) Math.ceil(((double) getAdjustmentIntervalMillis())
				/ getAggregationIntervalMillis());
	}
}
