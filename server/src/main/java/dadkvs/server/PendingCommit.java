package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.stub.StreamObserver;

class PendingCommit {
    DadkvsMain.CommitRequest request;
    StreamObserver<DadkvsMain.CommitReply> responseObserver;

    PendingCommit(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
        this.request = request;
        this.responseObserver = responseObserver;
    }
}
