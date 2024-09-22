package dadkvs.server;

/* these imported classes are generated by the contract */
import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import dadkvs.DadkvsSequencer;
import dadkvs.DadkvsSequencerServiceGrpc;

import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {

	DadkvsServerState server_state;
	int timestamp;
	int n_servers;
	private final DadkvsSequencerServiceGrpc.DadkvsSequencerServiceBlockingStub sequencerStub;
	private final ManagedChannel sequencerChannel;
	private final ManagedChannel[] serverChannels;
	private final DadkvsMainServiceGrpc.DadkvsMainServiceStub[] serverStubs;
	private int port;
	private String host;
	private String[] targets;

	public DadkvsMainServiceImpl(DadkvsServerState state) {
		this.server_state = state;
		this.timestamp = 0;
		this.n_servers = 5;
		this.sequencerChannel = ManagedChannelBuilder.forAddress("localhost", 8090).usePlaintext().build();
		this.sequencerStub = DadkvsSequencerServiceGrpc.newBlockingStub(sequencerChannel);
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
	}

	@Override
	public void read(DadkvsMain.ReadRequest request, StreamObserver<DadkvsMain.ReadReply> responseObserver) {

		if(server_state.checkFrozenOrDelay()) {
			DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Server is frozen, cannot process read request\n");
			return;
		}

		// for debug purposes
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
				"Receiving read request with reqid %d and key %d\n", request.getReqid(), request.getKey());

		int reqid = request.getReqid();
		int key = request.getKey();
		VersionedValue vv = this.server_state.store.read(key);

		DadkvsMain.ReadReply response = DadkvsMain.ReadReply.newBuilder()
				.setReqid(reqid).setValue(vv.getValue()).setTimestamp(vv.getVersion()).build();
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
				"Sending read reply with value %d and timestamp %d\n\n", vv.getValue(), vv.getVersion());
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void committx(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {

		if(server_state.checkFrozenOrDelay()) {
			DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Server is frozen, cannot process commit request\n");
			return;
		}
		// for debug purposes
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
				"Receiving commit request with reqid %d to read keys %d and %d and write key %d with value %d\n",
				request.getReqid(), request.getKey1(), request.getKey2(), request.getWritekey(), request.getWriteval());
		boolean result;
		int sequenceNumber = -1;
		int reqId = request.getReqid();

		if (this.server_state.isLeader()) {
			// gets the request sequence number from the sequencer
			DadkvsSequencer.GetSeqNumberRequest seqRequest = DadkvsSequencer.GetSeqNumberRequest.newBuilder().build();
			DadkvsSequencer.GetSeqNumberResponse seqResponse = this.sequencerStub.getSeqNumber(seqRequest);
			sequenceNumber = seqResponse.getSeqNumber();
			DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "SeqNumber is %d\n", sequenceNumber);
			// sends the request to all servers
			sendToReplicas(sequenceNumber, reqId);
		}
		this.timestamp++;
		result = this.server_state.processTransaction(request, sequenceNumber, this.timestamp);
		if (result) {
			DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Transaction committed successfully for reqid %d\n", reqId);
		} else {
			DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Transaction failed for reqid %d\n", reqId);
		}
		// for debug purposes
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Sending commit reply for reqid %d\n\n", reqId);
		DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
				.setReqid(reqId).setAck(result).build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void sequenceNumber(DadkvsMain.SequenceNumberRequest request,
			StreamObserver<DadkvsMain.SequenceNumberResponse> responseObserver) {
	

		int seqNumber = request.getSeqnumber();
		int reqId = request.getReqid();
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
				"[ServiceImpl] Received sequence number request with seqNumber " + seqNumber + " and reqId " + reqId);
		this.server_state.updateSequenceNumber(reqId, seqNumber);
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
				"[ServiceImpl] Sequence number updated for reqId " + reqId);
		DadkvsMain.SequenceNumberResponse response = DadkvsMain.SequenceNumberResponse.newBuilder().setReqid(reqId)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	private void sendToReplicas(int seqNumber, int reqId) {

		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(), "Sending request to replicas\n");
		DadkvsMain.SequenceNumberRequest sequenceNumberRequest = DadkvsMain.SequenceNumberRequest.newBuilder()
				.setSeqnumber(seqNumber).setReqid(reqId).build();
		ArrayList<DadkvsMain.SequenceNumberResponse> sequenceNumberResponses = new ArrayList<>();
		GenericResponseCollector<DadkvsMain.SequenceNumberResponse> sequenceNumberCollector = new GenericResponseCollector<>(
				sequenceNumberResponses, n_servers);
		for (DadkvsMainServiceGrpc.DadkvsMainServiceStub stub : serverStubs) {
			CollectorStreamObserver<DadkvsMain.SequenceNumberResponse> seqNumObserver = new CollectorStreamObserver<>(
					sequenceNumberCollector);
			stub.sequenceNumber(sequenceNumberRequest, seqNumObserver);
		}
		sequenceNumberCollector.waitForTarget(n_servers);
		DadkvsServer.debug(DadkvsMainServiceImpl.class.getSimpleName(),
				"Received all responses from replicas\n");
	}

}
