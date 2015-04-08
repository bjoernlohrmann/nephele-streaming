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
	 * Name of the configuration entry which defines the QoS statistics log file
	 * location.
	 */
	public static final String QOS_STAT_LOGFILE_PATTERN_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.logging.qos_statistics_filepattern");

	public static final String DEFAULT_QOS_STAT_LOGFILE_PATTERN = "/tmp/qos_statistics_%s";

	/**
	 * Name of the configuration entry which defines the CPU statistics log file
	 * location.
	 */
	private static final String CPU_STAT_LOGFILE_PATTERN_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.logging.cpu_statistics_filepattern");

	private static final String DEFAULT_CPU_STAT_LOGFILE_PATTERN = "/tmp/cpu_statistics_%s";

	/**
	 * Name of the configuration entry which defines the time interval for QoS
	 * driven adjustments.
	 */
	public static final String QOSMANAGER_ADJUSTMENTINTERVAL_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.adjustmentinterval");

	public static final long DEFAULT_ADJUSTMENTINTERVAL = 5000;

	/**
	 * For constraint enforcing, the time available for each edge is split into
	 * available output batching and queuing time in a fixed ratio. The ratio can be adjusted using
	 * the below "outputBatchingLatencyWeight" setting, which is a float in the range (0;1). If the
	 * outputBatchingLatencyWeight is set to 0.8, then the weight of queueing latency is (1-0.8) = 0.2
	 */
	public static final String QOSMANAGER_OUTPUT_BATCHING_WEIGHT_KEY = PluginManager
					.prefixWithPluginNamespace("streaming.qosmanager.output_batching_latency_weight");

	public static final float DEFAULT_QOSMANAGER_OUTPUT_BATCHING_WEIGHT = 0.8f;


	/**
	 * In elastic scaling, the measured queueing latency usually deviates from the one predicted
	 * by the G/G/1 queueing model. A fitting factor is used to fit the prediction to the current
	 * measurement point. The fitting factor should be close  to prevent under-/ overscaling.
	 * This setting specifies the limit of allowed deviation from 1 of the fitting factor. If set to x, then
	 * the allowed range of the fitting factor becomes (1-x;1+x).
	 */
	public static final String QOSMANAGER_SCALING_FITTING_FACTOR_DEVIATION_LIMIT_KEY = PluginManager
					.prefixWithPluginNamespace("streaming.qosmanager.scaling.fitting_factor_deviation_limit");

	public static final float DEFAULT_QOSMANAGER_SCALING_FITTING_FACTOR_DEVIATION_LIMIT = 0.2f;

	/**
	 * Poolsize of thread pool used for flushing output channels. It is better to err on the
	 * high side here, because setting this too low causes buffers to not get flushed in
	 * time for their deadline.
	 */
	public static final String OUTPUT_CAHNNEL_FLUSHER_THREADPOOLSIZE_KEY = PluginManager
					.prefixWithPluginNamespace("streaming.runtime.output_channel_flusher_threadpoolsize");

	public static final int DEFAULT_OUTPUT_CAHNNEL_FLUSHER_THREADPOOLSIZE = 20;

	/**
	 * Keep history of last 15min by default: 15 60 /
	 * (DEFAULT_ADJUSTMENTINTERVAL / 1000)) = 180
	 */
	public static final String IN_MEMORY_LOG_ENTRIES_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.logging.in_memory_entries");

	public static final int DEFAULT_IN_MEMORY_LOG_ENTRIES = 180;

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

	public static int getNoOfInMemoryLogEntries() {
		return GlobalConfiguration.getInteger(IN_MEMORY_LOG_ENTRIES_KEY,
				DEFAULT_IN_MEMORY_LOG_ENTRIES);
	}

	public static String getQosStatisticsLogfilePattern() {
		return GlobalConfiguration.getString(QOS_STAT_LOGFILE_PATTERN_KEY,
				DEFAULT_QOS_STAT_LOGFILE_PATTERN);
	}

	public static String getCpuStatisticsLogfilePattern() {
		return GlobalConfiguration.getString(CPU_STAT_LOGFILE_PATTERN_KEY,
				DEFAULT_CPU_STAT_LOGFILE_PATTERN);
	}

	public static int computeQosStatisticWindowSize() {
		return (int) Math.ceil(((double) getAdjustmentIntervalMillis())
				/ getAggregationIntervalMillis());
	}

	public static int getOutputChannelFlusherThreadpoolsize() {
		return GlobalConfiguration.getInteger(OUTPUT_CAHNNEL_FLUSHER_THREADPOOLSIZE_KEY,
						DEFAULT_OUTPUT_CAHNNEL_FLUSHER_THREADPOOLSIZE);
	}

	public static float getOutputBatchingLatencyWeight() {
		return GlobalConfiguration.getFloat(QOSMANAGER_OUTPUT_BATCHING_WEIGHT_KEY,
						DEFAULT_QOSMANAGER_OUTPUT_BATCHING_WEIGHT);
	}

	public static float getElasticScalingMaxFittingFactor() {
		return 1.0f + GlobalConfiguration.getFloat(QOSMANAGER_SCALING_FITTING_FACTOR_DEVIATION_LIMIT_KEY,
						DEFAULT_QOSMANAGER_SCALING_FITTING_FACTOR_DEVIATION_LIMIT);
	}

	public static float getElasticScalingMinFittingFactor() {
		return 1.0f - GlobalConfiguration.getFloat(QOSMANAGER_SCALING_FITTING_FACTOR_DEVIATION_LIMIT_KEY,
						DEFAULT_QOSMANAGER_SCALING_FITTING_FACTOR_DEVIATION_LIMIT);
	}
}
