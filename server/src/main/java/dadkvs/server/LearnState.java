package dadkvs.server;

public class LearnState {
    // LearnState -> learnCounter
    private int reqID;
    private int instanceNumber;
    private int roundNumber;

    public LearnState(int reqID, int instanceNumber, int roundNumber) {
        this.reqID = reqID;
        this.instanceNumber = instanceNumber;
        this.roundNumber = roundNumber;
    }

    public int getReqID() {
        return reqID;
    }

    public int getInstanceNumber() {
        return instanceNumber;
    }

    public int getRoundNumber() {
        return roundNumber;
    }

    public void setReqID(int reqID) {
        this.reqID = reqID;
    }

    public void setInstanceNumber(int instanceNumber) {
        this.instanceNumber = instanceNumber;
    }

    public void setRoundNumber(int roundNumber) {
        this.roundNumber = roundNumber;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof LearnState)) {
            return false;
        }
        LearnState learnState = (LearnState) obj;
        return learnState.reqID == reqID && learnState.instanceNumber == instanceNumber
                && learnState.roundNumber == roundNumber;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + reqID;
        result = 31 * result + instanceNumber;
        result = 31 * result + roundNumber;
        return result;
    }

}
