package dadkvs.server;

import java.util.HashMap;
import java.util.Map;

public class DadkvsServerState {
    boolean        i_am_leader;
    int            debug_mode;
    int            base_port;
    int            my_id;
    int            store_size;
    KeyValueStore  store;
    MainLoop       main_loop;
    Thread         main_loop_worker;
	private final SequenceNumber sequenceNumber;
	private final Map<Integer, Integer> pendingCommits;

	private class SequenceNumber {
		private int seqNum = 1;

		public synchronized int getSequenceNumber() { return this.seqNum; }

		public synchronized void incrementSequenceNumber() {
			this.seqNum++;
			this.notifyAll();
		}
	}

    
    public DadkvsServerState(int kv_size, int port, int myself) {
	base_port = port;
	my_id = myself;
	i_am_leader = my_id == 1;
	debug_mode = 0;
	store_size = kv_size;
	this.sequenceNumber = new SequenceNumber();
	this.pendingCommits = new HashMap<>();
	store = new KeyValueStore(kv_size);
	main_loop = new MainLoop(this);
	main_loop_worker = new Thread (main_loop);
	main_loop_worker.start();
    }

    public boolean isLeader() { return i_am_leader; }

	public int getSequenceNumber() { return this.sequenceNumber.getSequenceNumber(); }

	public void incrementSequenceNumber() {
		this.sequenceNumber.incrementSequenceNumber();
	}

	public void waitForTurn(int seqNum) throws InterruptedException {
		System.out.println("[DadkvsServerState] Waiting for sequence number: " + seqNum);
		synchronized (this.sequenceNumber) {
			while (this.sequenceNumber.getSequenceNumber() != seqNum) {
					System.err.println("Waiting for sequence number: " + seqNum + " but current sequence number is " + this.sequenceNumber.getSequenceNumber());
					this.sequenceNumber.wait();
				}
			}
		}

		public void updateSequenceNumber(int reqid, int seqNumber) {
			synchronized (this.pendingCommits) {
				this.pendingCommits.put(reqid, seqNumber);
				this.pendingCommits.notifyAll();
			}
		}

		public int getSequenceNumberForRequest(int reqid) {
			synchronized (this.pendingCommits) {
				return this.pendingCommits.getOrDefault(reqid, -1);
			}
		}

}
