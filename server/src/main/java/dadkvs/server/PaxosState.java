package dadkvs.server;

public class PaxosState {
    private int currentRoundNumber;
    private int currentReqId;
    private int readTs; // read_ts -> when we do PROMISE(n = roundNumber), we need to store the roundNumber of the last leader that we promised to
    private int writeTs; // write_ts -> when we accept a value, we store the roundNumber of the leader who we accepted the value from
    //int previousAcceptedReqId; // the reqId that was accepted

    public PaxosState(int currentRoundNumber, int currentReqId, int readTs, int writeTs) {
        this.currentRoundNumber = currentRoundNumber;
        this.currentReqId = currentReqId;
        this.readTs = readTs;
        this.writeTs = writeTs;

        /* // create entry on the learn counter map
        LearnState learnState = new LearnState(currentReqId, paxosCounter, currentRoundNumber);
        learnCounter.put(learnState, 0); */

    }

    public int getCurrentRoundNumber() {
        return currentRoundNumber;
    }

    public void setCurrentRoundNumber(int currentRoundNumber) {
        this.currentRoundNumber = currentRoundNumber;
    }

    public void setCurrentReqId(int currentReqId) {
        this.currentReqId = currentReqId;
    }

    public void setReadTs(int readTs) {
        this.readTs = readTs;
    }

    public void setWriteTs(int writeTs) {
        this.writeTs = writeTs;
    }

    public int getCurrentReqId() {
        return currentReqId;
    }

    public int getReadTs() {
        return readTs;
    }

    public int getWriteTs() {
        return writeTs;
    }

}
