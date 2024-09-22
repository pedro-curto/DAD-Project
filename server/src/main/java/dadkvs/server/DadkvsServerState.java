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
	debug_mode = 6;
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
						DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
								"Waiting for sequence number for reqid: %d\n", reqid);
						try {
							this.pendingCommits.wait();
						} catch (InterruptedException e) {
							DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
									"Error waiting for sequence number: %s\n", e.getMessage());
						}
					}
				}
			}
		}
		// waits to execute request based on its sequence number
		try {
			waitForTurn(sequenceNumber);
		} catch (InterruptedException e) {
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Error waiting for turn: %s\n", e.getMessage());
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
		DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Waiting for sequence number: %d\n", seqNum);
		synchronized (this.sequenceNumber) {
			while (this.sequenceNumber.getSequenceNumber() != seqNum) {
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
						"Waiting for sequence number: %d but current sequence number is %d\n", seqNum, this.sequenceNumber.getSequenceNumber());
				this.sequenceNumber.wait();
			}
		}
		DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Sequence number %d is ready\n", seqNum);
	}

	public void updateSequenceNumber(int reqid, int seqNumber) {
		synchronized (this.pendingCommits) {
			this.pendingCommits.put(reqid, seqNumber);
			this.pendingCommits.notifyAll();
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Sequence number %d updated for request with reqid %d\n", seqNumber, reqid);
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Current pendingCommits list: %s\n", this.pendingCommits);
		}
	}

	public int getSequenceNumberForRequest(int reqid) {
		synchronized (this.pendingCommits) {
			return this.pendingCommits.getOrDefault(reqid, -1);
		}
	}

		public boolean isServerFrozen() {
			return this.debug_mode == 2;
		}

}
