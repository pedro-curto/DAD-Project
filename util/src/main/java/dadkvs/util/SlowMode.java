package dadkvs.util;

public class SlowMode {
    
    private Boolean slow;

    public SlowMode() {
        this.slow = false;
    }

    public void slowOn() {
        synchronized (this) {
            this.slow = true;
        }
        System.out.println("Slowing server...");
    }

    public void slowOff() {
        synchronized (this) {
            this.slow = false;
            this.notifyAll();
        }
        System.out.println("Unslowing server...");
    }
    
    public void waitUntilUnslowed() {
        synchronized (this) {
            if (this.slow) {
                try {
                    waitRandomTime();
                } catch (Exception e) {
                    System.err.println("Error waiting for unslow: " + e.getMessage());
                }
            }
        }
    }

    public synchronized void waitRandomTime() {
		try {
			int randomTime = (int) (Math.random() * 500) + 500;
            System.out.println("Waiting random time (ms): " + randomTime);
			Thread.sleep(randomTime);
		} catch (InterruptedException e) {
			System.err.println("Error waiting random time: " + e.getMessage());
        }

	}
}
