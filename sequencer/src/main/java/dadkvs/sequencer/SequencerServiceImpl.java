package dadkvs.sequencer;

import io.grpc.stub.StreamObserver;

import dadkvs.DadkvsSequencer;
import dadkvs.DadkvsSequencer.GetSeqNumberResponse;
import dadkvs.DadkvsSequencerServiceGrpc;

public class SequencerServiceImpl extends DadkvsSequencerServiceGrpc.DadkvsSequencerServiceImplBase {

  int seqNumber;

  public SequencerServiceImpl() {
    seqNumber = 0;
  }

  @Override
  public synchronized void getSeqNumber(
      DadkvsSequencer.GetSeqNumberRequest request, StreamObserver<DadkvsSequencer.GetSeqNumberResponse> responseObserver) {
    seqNumber++;
    DadkvsSequencer.GetSeqNumberResponse response =
        GetSeqNumberResponse.newBuilder().setSeqNumber(seqNumber).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}