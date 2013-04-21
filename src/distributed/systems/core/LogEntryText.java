package distributed.systems.core;

public class LogEntryText {
	private String hostname;
	private int port;
	private LogEntryType event; 
	private int[] clock;
	private long jobID;

	public LogEntryText(String hostname,int port,LogEntryType event,int[] clock,long jobID) {
		this.hostname = hostname;
		this.port = port;
		this.event = event;
		this. clock = clock;
		this.jobID = jobID;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public LogEntryType getEvent() {
		return event;
	}

	public void setEvent(LogEntryType event) {
		this.event = event;
	}

	public int[] getClock() {
		return clock;
	}

	public void setClock(int[] clock) {
		this.clock = clock;
	}

	public long getJobID() {
		return jobID;
	}

	public void setJobID(int jobID) {
		this.jobID = jobID;
	}
	
	
}
