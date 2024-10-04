package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.GenericResponseCollector;
import dadkvs.DadkvsPaxos;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	private final Map<Integer, DadkvsMain.CommitRequest> pendingCommits;
	private int currentRoundCounter = 0;
	private final List<Integer> totalOrderList = new ArrayList();

	private final ManagedChannel[] serverChannels;
	private final DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] paxosStubs;
	private String[] targets;
	private String host;
	private int n_servers;
	private int n_acceptors;
	private int n_proposers;

	private int currentReqId = 0;
	private int timestamp = 0;
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
		// send prepare
		int roundNumber = generateRoundNumber();
		DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
				"Generated round number for paxos: %d", roundNumber);
		DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Going to run phase 1");

		boolean phaseOneResult = runPaxosPhase1(roundNumber, totalOrderList.size(), request.getReqid());
		DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Phase 1 result: %b", phaseOneResult);

		if (phaseOneResult) {
			// send accept
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Going to run phase 2");
			boolean phaseTwoResult = runPaxosPhase2(roundNumber, request.getReqid(), request);
			if (phaseTwoResult) {
				// learn
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Going to run learn");
				boolean learnResult = learn(roundNumber, request.getReqid());
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Learn result: %b", learnResult);
				return learnResult;
			}
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Phase 2 result: %b", phaseTwoResult);
			return phaseTwoResult;
		}
		return false;
	}

	public boolean runPaxosPhase1(int roundNumber, int index, int reqId) {
		int majority = (n_acceptors / 2) + 1;

		// constructs request
		DadkvsPaxos.PhaseOneRequest phaseOneRequest = DadkvsPaxos.PhaseOneRequest.newBuilder()
				.setPhase1RoundNumber(roundNumber)
				.setPhase1Index(index)
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
					"Sending PREPARE of round number %d to write on index %d to acceptor %d\n", roundNumber, index, i);
			paxosStub.phaseone(phaseOneRequest, phaseOneObserver);
		}

		// waits for majority of replies
		phaseOneCollector.waitForTarget(majority);

		// check if majority of replies are received
		int promisesCounter = 0;
		int new_reqId = reqId; // let's check if there is a greater one
		int maxTimeStamp = this.timestamp;
		for (DadkvsPaxos.PhaseOneReply reply : phaseOneReplies) {
			if (reply.getPhase1Accepted()) {
				promisesCounter++;

				// check if a promise has a greater timestamp in which case adopt its
				// reqid/value
				if (reply.getPhase1Timestamp() > maxTimeStamp) {
					maxTimeStamp = reply.getPhase1Timestamp();
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
			return true;
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

	public int checkTotalOrderIndexAvailability(int proposedIndex) {
		if (this.totalOrderList.size() == proposedIndex) {
			// the index is available
			return -1;
		} else {
			// the index is not available
			return this.totalOrderList.get(proposedIndex);
		}
	}

	public boolean isLeader() {
		return i_am_leader;
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

	public void addToPendingCommits(int reqId, DadkvsMain.CommitRequest request) {
		this.pendingCommits.put(reqId, request);
	}

	public int getCurrentTimestamp() {
		return this.timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}

	public synchronized void commitRequest(int learnreqid, int timestamp) {
		// if it's not in the pending commits, we don't commit since it was already
		// commited
		if (!this.pendingCommits.containsKey(learnreqid)) {
			return;
		}
		DadkvsMain.CommitRequest request = this.pendingCommits.get(learnreqid);
		TransactionRecord txRecord = new TransactionRecord(request.getKey1(), request.getVersion1(), request.getKey2(),
				request.getVersion2(), request.getWritekey(), request.getWriteval(), timestamp);
		boolean commitResult = this.store.commit(txRecord);
		if (commitResult) {
			// TODO correct to add to total order list here? shouldn't it be when it's
			// decided?
			this.totalOrderList.add(learnreqid);
			this.pendingCommits.remove(learnreqid);
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Transaction committed successfully for reqid %d\n", learnreqid);
		} else {
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Transaction failed to commit for reqid %d\n", learnreqid);
		}
	}
}
