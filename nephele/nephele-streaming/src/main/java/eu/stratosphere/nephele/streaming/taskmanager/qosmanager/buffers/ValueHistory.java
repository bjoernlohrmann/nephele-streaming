package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

/**
 * Stores a time-series of generic values. Instances of this class are usually
 * associated by the Qos data of a Qos edge.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class ValueHistory<T> {

	private Object[] entries;

	private int entriesInHistory;

	public ValueHistory(int noOfHistoryEntries) {
		this.entries = new Object[noOfHistoryEntries];
		this.entriesInHistory = 0;
	}

	public void addToHistory(long timestamp, T newValue) {
		HistoryEntry<T> newEntry = new HistoryEntry<T>(Math.min(
				this.entriesInHistory, this.entries.length - 1), timestamp,
				newValue);

		if (this.entriesInHistory < this.entries.length) {
			this.entries[this.entriesInHistory] = newEntry;
			this.entriesInHistory++;
		} else {
			System.arraycopy(this.entries, 1, this.entries, 0,
					this.entriesInHistory - 1);
			this.entries[this.entriesInHistory - 1] = newEntry;
		}
	}

	@SuppressWarnings("unchecked")
	public HistoryEntry<T> getFirstEntry() {
		return (HistoryEntry<T> ) this.entries[0];
	}

	@SuppressWarnings("unchecked")
	public HistoryEntry<T> getLastEntry() {
		if (this.entriesInHistory > 0) {
			return (HistoryEntry<T>) this.entries[this.entriesInHistory - 1];
		}

		return null;
	}

	public boolean hasEntries() {
		return this.entriesInHistory > 0;
	}

	public int getNumberOfEntries() {
		return this.entriesInHistory;
	}
}
