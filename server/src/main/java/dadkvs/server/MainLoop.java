package dadkvs.server;

public class MainLoop implements Runnable {
	DadkvsServerState server_state;

	private boolean has_work;

	public MainLoop(DadkvsServerState state) {
		this.server_state = state;
		this.has_work = false;
	}

	public void run() {
		while (true)
			this.doWork();
	}

	synchronized public void doWork() {
		System.out.println("Main loop do work start");
		System.out.println("Am I the leader? " + this.server_state.isLeader());
		this.has_work = false;
		while (this.has_work == false) {
			System.out.println("Main loop do work: waiting");
			try {
				wait();

				// Debugging
				switch (this.server_state.debug_mode) {
					case 1:
						System.out.println("Server Crashed");
						System.exit(1);
						break;
					case 2:
						System.out.println("Server Frozen");
						this.has_work = false;
						break;
					case 3:
					
						System.out.println("Server Unfrozen");
						this.has_work = true;
						break;
					case 4:
						System.out.println("Slow Mode On");
						break;
					case 5:
						System.out.println("Slow Mode Off");
						break;
					default:
						break;
				}
			} catch (InterruptedException e) {
			}
		}
		System.out.println("Main loop do work finish");
	}

	synchronized public void wakeup() {
		this.has_work = true;
		notify();
	}
}
