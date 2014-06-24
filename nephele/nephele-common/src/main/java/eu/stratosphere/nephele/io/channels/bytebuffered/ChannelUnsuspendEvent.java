package eu.stratosphere.nephele.io.channels.bytebuffered;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;

public class ChannelUnsuspendEvent extends AbstractEvent {

	@Override
	public void write(DataOutput out) throws IOException {
		// Nothing to do here

	}

	@Override
	public void read(DataInput in) throws IOException {
		// Nothing to do here
	}
}
