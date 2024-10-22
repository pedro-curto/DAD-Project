package dadkvs.util;

public class FreezeMode {

  private Boolean freeze;

  public FreezeMode() {
    this.freeze = false;
  }

  public void freeze() {
    synchronized (this) {
      this.freeze = true;
    }
    System.out.println("Freezing server...");
  }

  public void unfreeze() {
    synchronized (this) {
      this.freeze = false;
      this.notifyAll();
    }
    System.out.println("Unfreezing server...");
  }

  public void waitUntilUnfreezed() {
    synchronized (this) {
      while (this.freeze) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          System.err.println("Error waiting for unfreeze: " + e.getMessage());
        }
      }
    }
  }
}