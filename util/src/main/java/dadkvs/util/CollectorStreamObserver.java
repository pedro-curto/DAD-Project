package dadkvs.util;

import io.grpc.stub.StreamObserver;

public class CollectorStreamObserver<T> implements StreamObserver<T> {

    dadkvs.util.GenericResponseCollector collector;
    boolean done;

    public CollectorStreamObserver(GenericResponseCollector c) {
        collector = c;
        done = false;
    }

    @Override
    public void onNext(T value) {
        // Handle the received response of type T
        System.out.println("[StreamObserver] Received response: " + value);
        if (done == false) {
            collector.addResponse(value);
            done = true;
        }
    }

    @Override
    public void onError(Throwable t) {
        // Handle error
        System.err.println("[StreamObserver] Error occurred: " + t.getMessage());
        if (done == false) {
            collector.addNoResponse();
            done = true;
        }
    }

    @Override
    public void onCompleted() {
        // Handle stream completion
        System.out.println("[StreamObserver] Stream completed");
        if (done == false) {
            collector.addNoResponse();
            done = true;
        }
    }
}
