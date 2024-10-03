package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.GenericResponseCollector;
import dadkvs.DadkvsPaxos;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import dadkvs.util.CollectorStreamObserver;

public class DadkvsServerState {
	boolean i_am_leader;
	int debug_mode;
	int base_port;
	int my_id;
	int store_size;
	KeyValueStore store;
	MainLoop main_loop;
	Thread main_loop_worker;
	private final Lock paxosLock = new ReentrantLock();
	private final Map<Integer, Integer> pendingCommits;
	private int currentRoundCounter = 0;

	private final ManagedChannel[] serverChannels;
	private final DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] paxosStubs;
	private String[] targets;
	private String host;
	private int n_servers;
	private int n_acceptors;
	private int n_proposers;

	private int currentReqId = 0;
	private int latestAcceptedRoundNumber = 0;

	public DadkvsServerState(int kv_size, int port, int myself) {
		base_port = port;
		my_id = myself;
		i_am_leader = my_id == 1;
		debug_mode = 6;
		store_size = kv_size;
		this.pendingCommits = new HashMap<>();
		store = new KeyValueStore(kv_size);
		main_loop = new MainLoop(this);
		main_loop_worker = new Thread(main_loop);
		main_loop_worker.start();

		// communication with other servers
		this.n_servers = 5;
		this.n_acceptors = 3;
		this.n_proposers = 3;
		this.host = "localhost";
		this.targets = new String[n_servers];
		this.serverChannels = new ManagedChannel[n_servers];
		this.paxosStubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[n_servers];
		for (int i = 0; i < n_servers; i++) {
			int target_port = port + i;
			targets[i] = host + ":" + target_port;
			serverChannels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
			paxosStubs[i] = DadkvsPaxosServiceGrpc.newStub(serverChannels[i]);
		}
	}

	public boolean runPaxos(DadkvsMain.CommitRequest request) {
		// TODO -> implement Paxos algorithm
		synchronized (paxosLock) {
			// check if server is leader
			if (!this.isLeader()) {
				// should wait for leader to process request
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
						"Server is not the leader, waiting for leader to process request");
						
			}
			// check if server is the leader
			if (this.isLeader()) {
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
						"Server is the leader, processing paxos request");
				// send prepare
				int roundNumber = generateRoundNumber();
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
						"Generated round number for paxos: %d", roundNumber);
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Going to run phase 1");
				boolean phaseOneResult = runPaxosPhase1(roundNumber);
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Phase 1 result: %b", phaseOneResult);

				if (phaseOneResult) {
					// send accept
					DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Going to run phase 2");
					boolean phaseTwoResult = runPaxosPhase2(roundNumber, request.getReqid(), request);
					DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Phase 2 result: %b", phaseTwoResult);
					return phaseTwoResult;
				}
			}
			return false;
		}
	}

	public boolean runPaxosPhase1(int roundNumber) {
		int majority = (n_acceptors / 2) + 1;

		// constructs request
		DadkvsPaxos.PhaseOneRequest phaseOneRequest = DadkvsPaxos.PhaseOneRequest.newBuilder()
				.setPhase1RoundNumber(roundNumber)
				.build();

		ArrayList<DadkvsPaxos.PhaseOneReply> phaseOneReplies = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseOneCollector = new GenericResponseCollector<>(
				phaseOneReplies, n_acceptors);

		// sends PREPARE to all acceptors
		for (int i = 0; i < n_acceptors; i++) {
			DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub paxosStub = paxosStubs[i];
			CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseOneObserver = new CollectorStreamObserver<>(
					phaseOneCollector);

			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Sending PREPARE of round number %d to acceptor %d\n", roundNumber, i);
			paxosStub.phaseone(phaseOneRequest, phaseOneObserver);
		}

		// waits for majority of replies
		phaseOneCollector.waitForTarget(majority);

		// check if majority of replies are received
		int promisesCounter = 0;
		int new_reqId = -1;
		int maxRepliedRoundNumber = -1;
		for (DadkvsPaxos.PhaseOneReply reply : phaseOneReplies) {
			if (reply.getPhase1Accepted()) {
				promisesCounter++;

				if (reply.getPhase1RoundNumber() > maxRepliedRoundNumber) {
					maxRepliedRoundNumber = reply.getPhase1RoundNumber();
					new_reqId = reply.getPhase1Reqid();
				}
			} else {
				// TODO: RETRY AGAIN WITH ROUND NUMBER = ROUND NUMBER + 1
			}
		}
		if (promisesCounter >= majority) {
			DadkvsServer.debug(this.getClass().getSimpleName(),
					"Received majority of promises for round number " + roundNumber);
			// update to new write timestamp
			this.currentReqId = new_reqId;
		} else {
			DadkvsServer.debug(this.getClass().getSimpleName(),
					"Did not receive majority of promises for round number " + roundNumber);
		}

		return promisesCounter >= majority;
	}

	public boolean runPaxosPhase2(int roundNumber, int reqId, DadkvsMain.CommitRequest request) {
		int majority = (n_acceptors / 2) + 1;

		// constructs request
		DadkvsPaxos.PhaseTwoRequest phaseTwoRequest = DadkvsPaxos.PhaseTwoRequest.newBuilder()
				.setPhase2RoundNumber(roundNumber)
				.setPhase2Reqid(reqId)
				.build();

		ArrayList<DadkvsPaxos.PhaseTwoReply> phaseTwoReplies = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phaseTwoCollector = new GenericResponseCollector<>(
				phaseTwoReplies, n_acceptors);

		// sends ACCEPT to all acceptors
		for (int i = 0; i < n_acceptors; i++) {
			DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub paxosStub = paxosStubs[i];
			CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phaseTwoObserver = new CollectorStreamObserver<>(
					phaseTwoCollector);

			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Sending ACCEPT of round number %d to acceptor %d with reqid %d\n", roundNumber, i, reqId);

			paxosStub.phasetwo(phaseTwoRequest, phaseTwoObserver);
		}

		// waits for majority of replies
		phaseTwoCollector.waitForTarget(majority);

		// check if majority of replies are received
		int acceptsCounter = 0;
		for (DadkvsPaxos.PhaseTwoReply reply : phaseTwoReplies) {
			if (reply.getPhase2Accepted()) {
				acceptsCounter++;
			}
		}
		if (acceptsCounter >= majority) {
			DadkvsServer.debug(this.getClass().getSimpleName(),
					"Received majority of accepts for round number " + roundNumber);
			// writet the key
			TransactionRecord txRecord = new TransactionRecord(request.getKey1(), request.getVersion1(),
					request.getKey2(),
					request.getVersion2(), request.getWritekey(), request.getWriteval());
			boolean commitResult = this.store.commit(txRecord);
			return commitResult;
		} else {
			DadkvsServer.debug(this.getClass().getSimpleName(),
					"Did not receive majority of accepts for round number " + roundNumber);
		}

		return acceptsCounter >= majority;
	}

	public boolean learn(int roundNumber, int reqId) {
		// constructs request
		int majority = (n_servers / 2) + 1;

		DadkvsPaxos.LearnRequest learnRequest = DadkvsPaxos.LearnRequest.newBuilder()
				.setLearnroundnumber(roundNumber)
				.setLearnreqid(reqId)
				.build();

		ArrayList<DadkvsPaxos.LearnReply> learnReplies = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.LearnReply> learnCollector = new GenericResponseCollector<>(
				learnReplies, n_servers);

		// sends LEARN to all servers
		for (int i = 0; i < n_servers; i++) {
			DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub paxosStub = paxosStubs[i];
			CollectorStreamObserver<DadkvsPaxos.LearnReply> learnObserver = new CollectorStreamObserver<>(
					learnCollector);

			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Sending LEARN of round number %d to server %d with reqid %d\n", roundNumber, i, reqId);

			paxosStub.learn(learnRequest, learnObserver);
		}

		// waits for majority of replies
		learnCollector.waitForTarget(majority);

		// check if majority of replies are received
		int learnsCounter = 0;
		for (DadkvsPaxos.LearnReply reply : learnReplies) {
			if (reply.getLearnaccepted()) {
				learnsCounter++;
			}
		}
		if (learnsCounter >= majority) {
			DadkvsServer.debug(this.getClass().getSimpleName(),
					"Received majority of learns for round number " + roundNumber);
		} else {
			DadkvsServer.debug(this.getClass().getSimpleName(),
					"Did not receive majority of learns for round number " + roundNumber);
		}
		return learnsCounter >= majority;
	}

	
/* 	public boolean processTransaction(int reqId) {
		// get req with reqId from pendingCommits
		Integer request = this.pendingCommits.get(reqId);
		
	} */
	
/* 	 public boolean processTransaction(DadkvsMain.CommitRequest request, int
	  sequenceNumber, int timestamp) {
	  // TODO -> we need to remove the entry from the pendingCommits list after the
	  // transaction is processed
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
	  DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
	  "Error waiting for turn: %s\n", e.getMessage());
	  }
	  // commits transaction
	  TransactionRecord txRecord = new TransactionRecord(key1, version1, key2,
	  version2, writekey, writeval,
	  timestamp);
	  boolean commitResult = this.store.commit(txRecord);
	  this.incrementSequenceNumber();
	  return commitResult;
	  } */
	 

	public boolean isLeader() {
		return i_am_leader;
	}

	
/* 	  public int getSequenceNumber() {
	  return this.sequenceNumber.getSequenceNumber();
	  }
	  
	  public void incrementSequenceNumber() {
	  this.sequenceNumber.incrementSequenceNumber();
	  }
	  
	  public void waitForTurn(int seqNum) throws InterruptedException {
	  DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
	  "Waiting for sequence number: %d\n", seqNum);
	  synchronized (this.sequenceNumber) {
	  while (this.sequenceNumber.getSequenceNumber() != seqNum) {
	  DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
	  "Waiting for sequence number: %d but current sequence number is %d\n",
	  seqNum,
	  this.sequenceNumber.getSequenceNumber());
	  this.sequenceNumber.wait();
	  }
	  }
	  DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
	  "Sequence number %d is ready\n", seqNum);
	  } */
	 

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

	public synchronized void waitRandomTime() {
		try {
			int randomTime = (int) (Math.random() * 3000) + 2000;
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Waiting random time (ms): %d\n", randomTime);
			Thread.sleep(randomTime);
		} catch (InterruptedException e) {
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Error waiting random time: %s\n",
					e.getMessage());
		}

	}

	public boolean checkFrozenOrDelay() {
		switch (this.debug_mode) {
			case 2:
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Server Frozen\n");
				return true;
			case 4:
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Server is waiting random time\n");
				waitRandomTime();
				return false;
			default:
				return false;
		}

	}

	private synchronized int generateRoundNumber() {
		// need to generate unique proposal numbers
		int newRound = currentRoundCounter * n_proposers + my_id;
		currentRoundCounter++;
		return newRound;
	}

	public int getLatestAcceptedRoundNumber() {
		return latestAcceptedRoundNumber;
	}

	public void setLatestAcceptedRoundNumber(int latestAcceptedRoundNumber) {
		this.latestAcceptedRoundNumber = latestAcceptedRoundNumber;
	}

	public int getCurrentReqId() {
		return currentReqId;
	}

	public void setCurrentReqId(int currentReqId) {
		this.currentReqId = currentReqId;
	}

	public ManagedChannel[] getServerChannels() {
		return serverChannels;
	}

	public DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] getPaxosStubs() {
		return paxosStubs;
	}

}
