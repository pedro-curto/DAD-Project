package dadkvs.server;


public class TransactionRecord {
    private int timestamp;
    private int read1_key;
    private int read1_version;
    private int read2_key;
    private int read2_version;
    private int prepare_key;
    private int prepare_value;

    public TransactionRecord() {
        this.read1_key = 0;
        this.read1_version = 0;
        this.read2_key = 0;
        this.read2_version = 0;
        this.prepare_key = 0;
        this.prepare_value = 0;
	this.timestamp = -1;
     }

    public TransactionRecord(int key1, int v1, int key2, int v2, int wkey, int wval) {
        this.read1_key = key1;
        this.read1_version = v1;
        this.read2_key = key2;
        this.read2_version = v2;
        this.prepare_key = wkey;
        this.prepare_value = wval;
	this.timestamp = -1;
    }

    public TransactionRecord(int key1, int v1, int key2, int v2, int wkey, int wval, int ts) {
        this.read1_key = key1;
        this.read1_version = v1;
        this.read2_key = key2;
        this.read2_version = v2;
        this.prepare_key = wkey;
        this.prepare_value = wval;
	this.timestamp = ts;
    }

    // Getter and Setter methods for all fields
    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public int getRead1Key() {
        return read1_key;
    }

    public void setRead1Key(int read1_key) {
        this.read1_key = read1_key;
    }

    public int getRead1Version() {
        return read1_version;
    }

    public void setRead1Version(int read1_version) {
        this.read1_version = read1_version;
    }

    public int getRead2Key() {
        return read2_key;
    }

    public void setRead2Key(int read2_key) {
        this.read2_key = read2_key;
    }

    public int getRead2Version() {
        return read2_version;
    }

    public void setRead2Version(int read2_version) {
        this.read2_version = read2_version;
    }

    public int getPrepareKey() {
        return prepare_key;
    }

    public void setPrepareKey(int prepare_key) {
        this.prepare_key = prepare_key;
    }

    public int getPrepareValue() {
        return prepare_value;
    }

    public void setPrepareValue(int prepare_value) {
        this.prepare_value = prepare_value;
    }
}
