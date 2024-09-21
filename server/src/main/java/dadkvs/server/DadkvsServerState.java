package dadkvs.server;

import dadkvs.DadkvsMain;

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

	public boolean processTransaction(DadkvsMain.CommitRequest request, int sequenceNumber, int timestamp) {
		// TODO -> we need to remove the entry from the pendingCommits list after the transaction is processed
		int reqid = request.getReqid();
		int key1 = request.getKey1();
		int version1 = request.getVersion1();
		int key2 = request.getKey2();
		int version2 = request.getVersion2();
		int writekey = request.getWritekey();
		int writeval = request.getWriteval();

		// waits for sequence number to be placed in the pendingCommits list
		if (!this.isLeader()) {
			synchronized (this.pendingCommits) {
				while (sequenceNumber == -1) {
					sequenceNumber = this.getSequenceNumberForRequest(reqid);
					if (sequenceNumber == -1) {
						System.out.println("[DadkvsServerState] Waiting for sequence number for reqid: " + reqid);
						try {
							this.pendingCommits.wait();
						} catch (InterruptedException e) {
							System.err.println("[DadkvsServerState] Error waiting for sequence number: " + e.getMessage());
						}
					}
				}
			}
		}
		// waits to execute request based on its sequence number
		try {
			waitForTurn(sequenceNumber);
		} catch (InterruptedException e) {
			System.err.println("[MainServiceImpl] Error waiting for turn: " + e.getMessage());
		}
		// commits transaction
		TransactionRecord txRecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval, timestamp);
		boolean commitResult = this.store.commit(txRecord);
		this.incrementSequenceNumber();
		return commitResult;
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
					System.err.println("[DadkvsServerState] Waiting for sequence number: " + seqNum + " but current sequence number is " + this.sequenceNumber.getSequenceNumber());
					this.sequenceNumber.wait();
				}
			}
			System.out.println("[DadkvsServerState] Sequence number " + seqNum + " is ready");
		}

		public void updateSequenceNumber(int reqid, int seqNumber) {
			synchronized (this.pendingCommits) {
				this.pendingCommits.put(reqid, seqNumber);
				this.pendingCommits.notifyAll();
				System.out.println("[DadkvsServerState] Current pendingCommits list: " + this.pendingCommits);
			}
		}

		public int getSequenceNumberForRequest(int reqid) {
			synchronized (this.pendingCommits) {
				return this.pendingCommits.getOrDefault(reqid, -1);
			}
		}

}
