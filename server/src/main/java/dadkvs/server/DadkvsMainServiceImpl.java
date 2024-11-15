package dadkvs.server;

import java.util.LinkedList;
import java.util.Queue;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {

	DadkvsServerState server_state;
	//int timestamp; // (paxosCounter) amount of transactions that have commited
	int n_servers;
	private final ManagedChannel[] serverChannels;
	private final DadkvsMainServiceGrpc.DadkvsMainServiceStub[] serverStubs;
	private int port;
	private String host;
	private String[] targets;

	private boolean isPaxosRunning;
	private Queue<PendingCommit> commitQueue;

	public DadkvsMainServiceImpl(DadkvsServerState state) {
		this.server_state = state;
		//this.timestamp = 0;
		this.n_servers = 5;
		this.port = 8080;
		this.host = "localhost";

		// create channels and stubs to communicate with other servers
		this.targets = new String[n_servers];
		this.serverChannels = new ManagedChannel[n_servers];
		this.serverStubs = new DadkvsMainServiceGrpc.DadkvsMainServiceStub[n_servers];
		for (int i = 0; i < n_servers; i++) {
			int target_port = port + i;
			targets[i] = host + ":" + target_port;
			serverChannels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
			serverStubs[i] = DadkvsMainServiceGrpc.newStub(serverChannels[i]);
		}

		this.commitQueue = new LinkedList<>();
		this.isPaxosRunning = false;
	}

	@Override
	public void read(DadkvsMain.ReadRequest request, StreamObserver<DadkvsMain.ReadReply> responseObserver) {
		this.server_state.getFreezeMode().waitUntilUnfreezed();
		this.server_state.getSlowMode().waitUntilUnslowed();

		// for debug purposes
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
				"Receiving read request with reqid %d and key %d\n", request.getReqid(), request.getKey());

		Context ctx = Context.current().fork();
		ctx.run(() -> {
			int reqid = request.getReqid();
			int key = request.getKey();
			VersionedValue vv = this.server_state.store.read(key);

			DadkvsMain.ReadReply response = DadkvsMain.ReadReply.newBuilder()
					.setReqid(reqid).setValue(vv.getValue()).setTimestamp(vv.getVersion()).build();
			DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
					"Sending read reply with value %d and timestamp %d\n\n", vv.getValue(), vv.getVersion());
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		});
	}

	@Override
	public void committx(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {

		this.server_state.getFreezeMode().waitUntilUnfreezed();
		this.server_state.getSlowMode().waitUntilUnslowed();

		// for debug purposes
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
				"Receiving commit request with reqid %d to read keys %d and %d and write key %d with value %d\n",
				request.getReqid(), request.getKey1(), request.getKey2(), request.getWritekey(), request.getWriteval());

		Context ctx = Context.current().fork();
		ctx.run(() -> {

			if (server_state.isLeader()) {
				synchronized(this) {
					if (isPaxosRunning()) {
						DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Paxos is running, adding request %d to queue\n", request.getReqid());
						commitQueue.add(new PendingCommit(request, responseObserver));
					} else {
						// starts new paxos instance
						isPaxosRunning = true;
						startPaxosForRequest(request, responseObserver);
					}
				}
			} else {
				int reqId = request.getReqid();
				DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "I am not the leader\n");
				this.server_state.addToPendingCommits(reqId, request);

				boolean result = this.server_state.waitForPaxosInstanceToFinish(reqId);

				if (!result) {
					// Became leader, need to start Paxos for this request
					synchronized (this) {
						if (isPaxosRunning()) {
							DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
									"Paxos is running, adding request %d to queue\n", request.getReqid());
							commitQueue.add(new PendingCommit(request, responseObserver));
						} else {
							isPaxosRunning = true;
							startPaxosForRequest(request, responseObserver);
						}
					}
					return;
				}

				DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Total order List: %s\n", this.server_state.getTotalOrderList());
				DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "RESULT OF PAXOS: %b\n", result);
				DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Sending commit reply for reqid %d\n\n",
						reqId);
				DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
						.setReqid(reqId).setAck(result).build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();
			}
		});
	}

	//public synchronized void incrementTimestamp() {
	//	this.timestamp++;
	//}

	private synchronized boolean isPaxosRunning() {
		return this.isPaxosRunning;
	}

	private void startPaxosForRequest(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
		// allows main to receive reads and add new commits to the queue
		new Thread(() -> {
			processCommitRequest(request, responseObserver);
		}).start();
	}

	private void processCommitRequest(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
		boolean result;
		int reqId = request.getReqid();

		this.server_state.addToPendingCommits(reqId, request);
		//this.server_state.setPaxosCounter(this.timestamp);
		
		result = this.server_state.runPaxos(request, true);
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Paxos number %d finished for request %d\n", server_state.getPaxosCounter(), reqId);
		DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
				.setReqid(reqId).setAck(result).build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	
		// processes next commit in the queue -> new paxos
		synchronized (this) {
			isPaxosRunning = false;
			if (!commitQueue.isEmpty()) {
				PendingCommit nextCommit = commitQueue.poll();
				isPaxosRunning = true;
				startPaxosForRequest(nextCommit.request, nextCommit.responseObserver);
			}
		}
	}
	
	


}
