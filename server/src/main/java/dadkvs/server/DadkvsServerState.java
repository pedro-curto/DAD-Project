package dadkvs.server;

public class DadkvsServerState {
    boolean        i_am_leader;
    int            debug_mode;
    int            base_port;
    int            my_id;
    int            store_size;
	int sequenceNumber;
    KeyValueStore  store;
    MainLoop       main_loop;
    Thread         main_loop_worker;

    
    public DadkvsServerState(int kv_size, int port, int myself) {
	base_port = port;
	my_id = myself;
	i_am_leader = my_id == 1;
	debug_mode = 0;
	store_size = kv_size;
	sequenceNumber = 0;
	store = new KeyValueStore(kv_size);
	main_loop = new MainLoop(this);
	main_loop_worker = new Thread (main_loop);
	main_loop_worker.start();
    }

    public boolean isLeader() { return i_am_leader; }

	public synchronized int getSequencenumber() { return sequenceNumber; }

	public synchronized void incrementSequenceNumber() {
		this.sequenceNumber++;
		this.notifyAll();
	}
}
