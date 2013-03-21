package distributed.systems.gridscheduler.model;

public class SyncLog {
	private boolean messageArrived;
	
	public SyncLog() {
		messageArrived = false;
		
	}
	
	public synchronized void check(){
		
		while(!messageArrived) {
			try {
				System.out.println("VAI ESTAR PRESo!!!!!!!!!!!!!");

				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("NAO ESTA PRESo!!!!!!!!!!!!!");
		
	}
	
	public synchronized void setArrived(){
		
		//if (!messageArrived){
			messageArrived = true;
			notifyAll();
		//}
	}

}
