package eu.stratosphere.nephele.io;

import eu.stratosphere.nephele.types.Record;

/**
 * This is the an additional implementation of the {@link ChannelSelector} interface.
 * It represents a variant of the simple round-robin strategy implemented in
 * {@link DefaultChannelSelector}. Opportunistic round-robin reduces the fan-out
 * overhead of simple round-robin, while keeping its load-balancing properties.
 * 
 * As in simple round robin, only one output channel at a time is selected for a
 * record, regardless of the record's properties. This channel is chosen in a
 * round-robin fashion from a set of channels. In simple round robin, this set
 * contains all (active) channels. In opportunistic round-robin, this set is
 * determined by picking the lowest possible number of channels so as to still
 * achieve round-robin's load-balancing properties (if all records have uniform
 * processing time AND are sent at the same rate by the sender tasks, then 
 * tasks on the receiver side will all have the same load).
 */
public class OpportunisticRoundRobinChannelSelector<T extends Record> implements
		ChannelSelector<T> {

	private final int[] channelIdxToReturn = new int[] { -1 };

	private final int numberOfSubtasks;

	private final int indexInSubtaskGroup;

	private int lastNumberOfOutputChannels = -1;
	private int channelsPerSubtask = -1;
	private int channelOffset = -1;
	private int currChannelIndex = -1;

	public OpportunisticRoundRobinChannelSelector(int numberOfSubtasks,
			int indexInSubtaskGroup) {
		this.numberOfSubtasks = numberOfSubtasks;
		this.indexInSubtaskGroup = indexInSubtaskGroup;
	}
	
	@Override
	public int[] selectChannels(T record, int numberOfOutputChannels) {

		if (lastNumberOfOutputChannels != numberOfOutputChannels) {
			lastNumberOfOutputChannels = numberOfOutputChannels;

			channelsPerSubtask = computeNoOfOutputChannelsToUse(numberOfOutputChannels);
			channelOffset = indexInSubtaskGroup * channelsPerSubtask;
		}

		currChannelIndex = (currChannelIndex + 1) % channelsPerSubtask;
		channelIdxToReturn[0] = (channelOffset + currChannelIndex)
				% numberOfOutputChannels;
		return channelIdxToReturn;
	}

	private int computeNoOfOutputChannelsToUse(int numberOfOutputChannels) {
		return leastCommonMultiple(numberOfSubtasks, numberOfOutputChannels)
				/ numberOfSubtasks;
	}

	private int greatestCommonDivisor(int x, int y) {
		while (y > 0) {
			int tmp = y;
			y = x % y;
			x = tmp;
		}
		return x;
	}

	private int leastCommonMultiple(int a, int b) {
		return a * (b / greatestCommonDivisor(a, b));
	}
}
