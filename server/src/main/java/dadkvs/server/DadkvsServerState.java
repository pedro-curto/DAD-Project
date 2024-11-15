package dadkvs.server;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.FreezeMode;
import dadkvs.util.GenericResponseCollector;
import dadkvs.util.SlowMode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class DadkvsServerState {
	boolean i_am_leader;
	int debug_mode;
	int base_port;
	int my_id;
	int store_size;
	KeyValueStore store;
	MainLoop main_loop;
	Thread main_loop_worker;
	FreezeMode freeze_mode;
	SlowMode slow_mode;

	// {301: requestDetails, 302: requestDetails, ...}
	private final Map<Integer, DadkvsMain.CommitRequest> pendingCommits;
	// [(reqId, true), (reqId2, false), ...]
	private final List<Map.Entry<Integer, Boolean>> totalOrderList;

	private final ManagedChannel[] serverChannels;
	private final DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] paxosStubs;
	private String[] targets;
	private String host;
	private int n_servers;
	private int n_acceptors;
	private int n_proposers;

	private Map<Integer, PaxosState> paxosInstances;
	private int paxosCounter; // for leader
	private int expectedInstanceNumber; // guarantee replicas apply the requests in the same order
	
	// MAPA [reqID, instanceNumber, roundNumber] ->>> learnCounter
	private Map<LearnState, Integer> learnCounter = new HashMap<>();


	public DadkvsServerState(int kv_size, int port, int myself) {
		base_port = port;
		my_id = myself;
		i_am_leader = my_id == 0;
		debug_mode = 6;
		store_size = kv_size;
		this.pendingCommits = new HashMap<>();
		store = new KeyValueStore(kv_size);
		main_loop = new MainLoop(this);
		main_loop_worker = new Thread(main_loop);
		main_loop_worker.start();
		freeze_mode = new FreezeMode();
		slow_mode = new SlowMode();
		
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

		this.totalOrderList = new ArrayList<>();
		this.paxosInstances = new ConcurrentHashMap<>();
		this.paxosCounter = 0; // counter for the paxos rounds
		this.expectedInstanceNumber = 1;
	}

	public DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] getAcceptors() {
		int currentConfig = getCurrentConfig();

		return new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] {
			paxosStubs[(currentConfig + 0) % n_servers],
			paxosStubs[(currentConfig + 1) % n_servers],
			paxosStubs[(currentConfig + 2) % n_servers]
		};

	}

	public DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] getProposers() {
		// since the proposers are the same as the acceptors, we just return the acceptors
		return getAcceptors();

	}


	public boolean runPaxos(DadkvsMain.CommitRequest request, boolean increment) {
		// increments the paxos counter, generates a round number 
		// and places the paxosState into the paxosInstances map (inside generateRoundNumber)
		int paxosInstance;
		if (increment) {
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Incrementing paxosCounter");
			paxosInstance = getNewPaxosInstance();
			System.out.println("2PaxosCounter: " + this.paxosCounter);
		} else {
			paxosInstance = this.paxosCounter;
		}
		int roundNumber = generateRoundNumber(paxosInstance); // round of paxos, one instance may have multiple rounds (each round starts with a PREPARE)
		//this.currentReqId = request.getReqid();
		DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
				"Starting paxos %d with round number %d and reqid %d", paxosInstance, roundNumber, request.getReqid());
		// sends PREPAREs		
		boolean phaseOneResult = runPaxosPhase1(roundNumber, request.getReqid(), paxosInstance);
		DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Phase 1 result: %b", phaseOneResult);

		if (phaseOneResult) {
			// send accept
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Going to run phase 2");
			int reqIdToPropose = this.paxosInstances.get(paxosInstance).getCurrentReqId();
			boolean phaseTwoResult = runPaxosPhase2(roundNumber, reqIdToPropose, request, paxosInstance);

			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Phase 2 result: %b", phaseTwoResult);
			return phaseTwoResult;
		}
		// PHASE ONE FAILED - NEED TO DO AN EXTRA ROUND
		waitExponentialBackoff();
		return runPaxos(request, false);
	}

	public boolean runPaxosPhase1(int roundNumber, int reqId, int paxosInstance) {
		int majority = (n_acceptors / 2) + 1;

		// constructs request
		DadkvsPaxos.PhaseOneRequest phaseOneRequest = DadkvsPaxos.PhaseOneRequest.newBuilder()
				.setPhase1RoundNumber(roundNumber)
				.setPhase1Index(paxosInstance)
				.setPhase1Config(this.getCurrentConfig())
				.build();

		ArrayList<DadkvsPaxos.PhaseOneReply> phaseOneReplies = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseOneCollector = new GenericResponseCollector<>(
				phaseOneReplies, n_acceptors);

		// sends PREPARE(n = roundNumber) to all acceptors
		int i = getCurrentConfig();
		for (DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub acceptor : getAcceptors()) {
			
			CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseOneObserver = new CollectorStreamObserver<>(
					phaseOneCollector);

			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Sending PREPARE of round number %d on paxosInstance %d to acceptor %d\n", roundNumber, paxosInstance, i);
			i++;
			acceptor.phaseone(phaseOneRequest, phaseOneObserver);
		}

		// waits for majority of replies
		phaseOneCollector.waitForTarget(majority);

		// check if majority of replies are received
		int promisesCounter = 0;
		// let's see if any write_ts if different from -1, if so, we adopt its reqId
		int new_reqId = reqId; // let's check if there is a greater one
		int maxReadTs = -1;
		for (DadkvsPaxos.PhaseOneReply reply : phaseOneReplies) {
			if (reply.getPhase1Accepted()) {
				promisesCounter++;

				// check if a promise has a greater timestamp in which case adopt its
				// reqid/value
				if (reply.getPhase1Timestamp() > maxReadTs) {
					DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
							"Found a greater timestamp: %d, replacing reqId %d with %d", reply.getPhase1Timestamp(), new_reqId, reply.getPhase1Reqid());
					maxReadTs = reply.getPhase1Timestamp();
					new_reqId = reply.getPhase1Reqid();
				}
			}
		}
		if (promisesCounter >= majority) {
			DadkvsServer.debug(this.getClass().getSimpleName(),
					"Received majority of promises for round number " + roundNumber);
			// sets the reqId in the paxosState to the reqId that was accepted
			this.paxosInstances.get(paxosInstance).setCurrentReqId(new_reqId);
		} else {
			DadkvsServer.debug(this.getClass().getSimpleName(),
					"Did not receive majority of promises for round number " + roundNumber);
		}

		return promisesCounter >= majority;
	}

	public boolean runPaxosPhase2(int roundNumber, int reqId, DadkvsMain.CommitRequest request, int paxosInstance) {
		int majority = (n_acceptors / 2) + 1;

		// constructs request
		DadkvsPaxos.PhaseTwoRequest phaseTwoRequest = DadkvsPaxos.PhaseTwoRequest.newBuilder()
				.setPhase2RoundNumber(roundNumber)
				.setPhase2Reqid(reqId)
				.setPhase2Index(paxosInstance)
				.setPhase2Config(this.getCurrentConfig())
				.build();

		ArrayList<DadkvsPaxos.PhaseTwoReply> phaseTwoReplies = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phaseTwoCollector = new GenericResponseCollector<>(
				phaseTwoReplies, n_acceptors);

		// sends ACCEPT to all acceptors
		int i = getCurrentConfig();
		for (DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub acceptor : getAcceptors()) {
			CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phaseTwoObserver = new CollectorStreamObserver<>(
					phaseTwoCollector);

			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Sending ACCEPT of round number %d to acceptor %d with reqid %d\n", roundNumber, i , reqId);
			i++;
			acceptor.phasetwo(phaseTwoRequest, phaseTwoObserver);
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
	

	public boolean learn(int roundNumber, int reqId, int paxosInstance) {
		// constructs request
		int majority = (n_acceptors / 2) + 1;

		DadkvsPaxos.LearnRequest learnRequest = DadkvsPaxos.LearnRequest.newBuilder()
				.setLearnroundnumber(roundNumber)
				.setLearnreqid(reqId)
				.setLearnindex(paxosInstance)
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


		// waits for replies
		learnCollector.waitForTarget(n_acceptors);

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


	public synchronized void commitRequest(int learnreqid, int paxosInstance) {
		// we check if the request is already in the total order list (already committed)
		for (Map.Entry<Integer, Boolean> entry : totalOrderList) {
			if (entry.getKey() == learnreqid) {
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Request with reqid %d is already in total order list", learnreqid);
				return;
			}
		}
		DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
		"Committing request with reqId %d in paxosInstance %d\n", learnreqid, paxosInstance);
		while (paxosInstance != expectedInstanceNumber) {
			try {
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
						"Waiting for paxos instance %d to finish", expectedInstanceNumber);
				wait();
			} catch (InterruptedException e) {
				// Handle exception
			}
		}
		// there is a case with a lagging replica that receives the learns of a request it doesn't know about yet
		// so we need to check if the request is in the pendingCommits; if it isn't, we wait for the commit to be placed

		while (!this.pendingCommits.containsKey(learnreqid)) {
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Request with reqid %d is not in pendingCommits, waiting for it", learnreqid);
			try {
				wait();
			} catch (InterruptedException e) {
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
						"Error waiting for request with reqid %d: %s\n", learnreqid, e.getMessage());
			}
		}
		DadkvsServer.debug(this.getClass().getSimpleName(),
        "Committing request with reqId: " + learnreqid +
        " | Global timestamp (Paxos Instance): " + paxosInstance);
		DadkvsMain.CommitRequest request = this.pendingCommits.get(learnreqid);
		TransactionRecord txRecord = new TransactionRecord(request.getKey1(), request.getVersion1(), request.getKey2(),
				request.getVersion2(), request.getWritekey(), request.getWriteval(), paxosInstance);
		boolean commitResult = this.store.commit(txRecord);
		if (commitResult) {
			if(txRecord.getPrepareKey() == 0){
				if (!canIPropose()) {
					i_am_leader = false;
				}
			}
			this.pendingCommits.remove(learnreqid);
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Transaction committed successfully for reqid %d\n", learnreqid);
		} else {
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
					"Transaction failed to commit for reqid %d\n", learnreqid);
		}
		// prints total order list after the commit
		totalOrderList.add(new AbstractMap.SimpleEntry<>(learnreqid, commitResult));
		DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Total order list: %s\n", this.totalOrderList);
		expectedInstanceNumber++;
		notifyAll();
	}

	public boolean canIPropose(){
		return my_id >= getCurrentConfig() && my_id < getCurrentConfig() + n_acceptors;
	}

	public int getCurrentConfig() {
		return store.getConfig();
	}

	public boolean isLeader() {
		return i_am_leader;
	}

	public void setLeader(boolean leader) {
		i_am_leader = leader;
		if (leader) {
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Setting leader and paxosCounter to %d\n", this.expectedInstanceNumber-1);
			this.paxosCounter = this.expectedInstanceNumber-1;
        	DadkvsServer.debug(DadkvsServerState.class.getSimpleName(),
                "Becoming leader. Setting paxosCounter to %d", this.paxosCounter);
		}
	}

	public boolean isServerFrozen() {
		return this.debug_mode == 2;

	}

	public int getPaxosCounter() {
		return this.paxosCounter;
	}

	// get LEARN counter
	public int getLearnCounter(int reqId, int roundNumber, int paxosInstance) {
		LearnState learnState = new LearnState(reqId, paxosInstance, roundNumber);
		if (learnCounter.containsKey(learnState)) {
			System.out.println("LearnCounter: " + learnCounter.get(learnState));
			// increment the value on the map learnCounter
			learnCounter.put(learnState, learnCounter.get(learnState) + 1);
			
			return learnCounter.get(learnState);
		}

		else {
			// create entry on the learn counter map
			learnCounter.put(learnState, 1);
			System.out.println("LearnCounter: " + learnCounter.get(learnState));
			return 1;
		}
	}

	public ManagedChannel[] getServerChannels() {
		return serverChannels;
	}

	public DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] getPaxosStubs() {
		return paxosStubs;
	}

	public synchronized void addToPendingCommits(int reqId, DadkvsMain.CommitRequest request) {
		this.pendingCommits.put(reqId, request);
		notifyAll();
	}

	// PAXOS METHODS

	private synchronized int generateRoundNumber(int paxosInstance) {
		// need to generate unique proposal numbers
		if (!this.paxosInstances.containsKey(paxosInstance)) {
			this.paxosInstances.put(paxosInstance, new PaxosState(my_id, -1, -1, -1));
			return my_id;
		}
		int previousRound = this.paxosInstances.get(paxosInstance).getCurrentRoundNumber();
		int newRound = previousRound + n_proposers;
		// sets the new round number in the paxosState
		this.paxosInstances.get(paxosInstance).setCurrentRoundNumber(newRound);
		return newRound;
	}

	public synchronized boolean waitForPaxosInstanceToFinish(int reqId) {

		// wait until the Paxos instance finishes or the leader changes
		while (totalOrderList.stream().noneMatch(entry -> entry.getKey() == reqId)) {
			if (isLeader()) {
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "I AM THE LEADER NOW");
				// became leader, transaction not committed yet
				return false;
			}
			
			try {
				System.err.println("Waiting for Paxos instance to finish");
				wait();

			} catch (InterruptedException e) {
				DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Error waiting for Paxos instance: %s\n",
						e.getMessage());
			}
			//}
	
			System.out.println("Finished waiting for Paxos instance");
		}
	
		return true;
	}


	public synchronized int getNewPaxosInstance() {
		System.out.println("1PaxosCounter: " + this.paxosCounter);
		return ++paxosCounter;
	}
	

	public synchronized void setPaxosCounter(int paxosCounter) {
		this.paxosCounter = paxosCounter;
	}

	public PaxosState getOrCreatePaxosState(int proposedRoundNumber, int paxosCounter) {
		if (!this.paxosInstances.containsKey(paxosCounter)) {
			this.paxosInstances.put(paxosCounter, new PaxosState(proposedRoundNumber, -1, -1, -1));
		}
		return this.paxosInstances.get(paxosCounter);
	}

	public List<Map.Entry<Integer, Boolean>> getTotalOrderList() {
		return this.totalOrderList;
	}

	public void waitExponentialBackoff() {
		// wait 5 seconds
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			DadkvsServer.debug(DadkvsServerState.class.getSimpleName(), "Error waiting exponential backoff: %s\n",
					e.getMessage());
		}
	}

	public int getNumberOfAcceptors() {
		return this.n_acceptors;
	}

	public FreezeMode getFreezeMode() {
		return this.freeze_mode;
	}

	public SlowMode getSlowMode() {
		return this.slow_mode;
	}
}
