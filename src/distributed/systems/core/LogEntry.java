package distributed.systems.core;

import java.net.InetSocketAddress;
import distributed.systems.gridscheduler.model.*;


public class LogEntry {

	private InetSocketAddress origin;
	private Long id; //Generic id can be JobId, ClusterId and GSId
	private String event; 
	private int[] clock;

	public LogEntry(ControlMessage M){

		this.setOrigin(M.getInetAddress());
		this.setEvent(M.getType().toString());

		if(M.getJob() != null)
			this.setId(M.getJob().getId());
		this.setClock(clock);


	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getEvent() {
		return event;
	}

	public void setEvent(String event) {
		this.event = event;
	}

	public int[] getClock() {
		return clock;
	}

	public void setClock(int[] clock) {
		this.clock = clock;
	}

	public InetSocketAddress getOrigin() {
		return origin;
	}

	public void setOrigin(InetSocketAddress origin) {
		this.origin = origin;
	}

	
	
//TODO public String toString(){}

}

