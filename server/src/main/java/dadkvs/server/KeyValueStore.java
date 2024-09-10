package dadkvs.server;

public class KeyValueStore {
    private int size;
    private VersionedValue[] values;

    public KeyValueStore(int n_entries) {
        this.size = n_entries;
        this.values = new VersionedValue[n_entries];
        for (int i = 0; i < n_entries; i++) {
            this.values[i] = new VersionedValue(0, 0);
        }
    }

    synchronized public VersionedValue read(int k) {
        if (k < size) {
            return values[k];
        } else {
            return null;
        }
    }

    synchronized public boolean write(int k, VersionedValue v) {
        if (k < size) {
            values[k] = v;
	    return true;
        }
	else
	    return false;
    }

    synchronized public boolean commit(TransactionRecord tr) {
	System.out.println("store commit read first key = " + tr.getRead1Key() + " with version = " + tr.getRead1Version()  + "  and current version = " + this.read(tr.getRead1Key()).getVersion());
 	System.out.println("store commit read second key = " + tr.getRead2Key()  + " with version = " + tr.getRead2Version()  + " and current version = " + this.read(tr.getRead2Key()).getVersion());
	System.out.println("store commit write key  " + tr.getPrepareKey()  + " with value = " + tr.getPrepareValue() + " and version " + tr.getTimestamp());
        if (this.read(tr.getRead1Key()).getVersion() == tr.getRead1Version() &&
            this.read(tr.getRead2Key()).getVersion() == tr.getRead2Version()) {
            VersionedValue vv = new VersionedValue(tr.getPrepareValue(), tr.getTimestamp());
            this.write(tr.getPrepareKey(), vv);
            return true;
        } else {
            return false;
        }
    }
}
