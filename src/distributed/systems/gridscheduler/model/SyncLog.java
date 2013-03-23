package distributed.systems.gridscheduler.model;

public class SyncLog {
	private boolean messageArrived;
	
	public SyncLog() {
		messageArrived = false;
		
	}
	
	public synchronized void check(){
		
		while(!messageArrived) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public synchronized void setArrived(){
		
		//if (!messageArrived){
			messageArrived = true;
			notifyAll();
		//}
	}

}
