package dadkvs.server;

public class VersionedValue {
    private int value;
    private int version;

    public VersionedValue(int val, int ver) {
        this.value = val;
        this.version = ver;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
