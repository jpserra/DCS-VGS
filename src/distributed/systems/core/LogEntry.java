package distributed.systems.core;

import java.io.Serializable;
import java.net.InetSocketAddress;
import distributed.systems.gridscheduler.model.*;


public class LogEntry implements Serializable {

	private static final long serialVersionUID = -8054991011970570003L;
	private InetSocketAddress origin;
	private Long id; //Generic id can be JobID, ClusterID and GSID
	private String event; 
	private int[] clock;

	public LogEntry(ControlMessage message){
		this.setOrigin(message.getInetAddress());
		this.setEvent(message.getType().toString());
		if(message.getJob() != null)
			this.setId(message.getJob().getId());
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

	@Override
	public String toString(){
		String s = "[";

		if(origin != null)
			s += "Origin: " + origin.getHostName()+ ":"+origin.getPort() + " | ";

		if(id != null)
			s += "ID: " + id + " | ";

		if(event != null)
			s += "Event: " + event + " | "; 

		if(clock != null)
			s += "VecClock: " + clock ;

		s+= "]";
		return s;

	}

}

