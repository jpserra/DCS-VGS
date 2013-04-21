package distributed.systems.core;

import java.io.Serializable;
import java.net.InetSocketAddress;
import distributed.systems.gridscheduler.model.ControlMessage;
import distributed.systems.gridscheduler.model.Job;


public class LogEntry implements Serializable {

	private static final long serialVersionUID = 8054991011970570003L;
	private InetSocketAddress origin;
	//	private Long id; //Generic id can be JobID, ClusterID and GSID
	private LogEntryType event; 
	private int[] clock;
	private Job job;

	public LogEntry(ControlMessage message){
		this.setOrigin(message.getInetAddress());
		this.setClock(message.getClock());
		if(message.getJob() != null)
			this.setJob(message.getJob());
		switch(message.getType().toString()){
			case "JobArrival":
				this.setEvent(LogEntryType.JOB_ARRIVAL);
				break;
			case "JobStarted":
				this.setEvent(LogEntryType.JOB_STARTED);
				break;
			case "JobCompleted":
				this.setEvent(LogEntryType.JOB_COMPLETED);
				break;
			case "GSLogJobArrival":
				this.setEvent(LogEntryType.JOB_ARRIVAL);
				break;
			case "GSLogJobStarted":
				this.setEvent(LogEntryType.JOB_STARTED);
				break;
			case "GSLogJobCompleted":
				this.setEvent(LogEntryType.JOB_COMPLETED);
				break;
			case "AddJobAck":
				this.setEvent(LogEntryType.JOB_DELEGATED);
				break;
			case "RestartRM":
				this.setEvent(LogEntryType.RESTART_RM);
				break;
			case "GSLogRestartRM":
				this.setEvent(LogEntryType.RESTART_RM);
				break;
			case "RestartGS":
				this.setEvent(LogEntryType.RESTART_GS);
				break;
			case "GSLogRestartGS":
				this.setEvent(LogEntryType.RESTART_GS);
				break;
			default: 		this.setEvent(LogEntryType.UNKNOWN);
		}
	}

	public LogEntry(Job j, LogEntryType event, int[] clock){
		this.setOrigin(null);
		this.setEvent(event);
		this.setJob(j);
		this.setClock(clock);
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

	public void setClock(int[] clock2) {
		this.clock = clock2;
	}

	public InetSocketAddress getOrigin() {
		return origin;
	}

	public void setOrigin(InetSocketAddress origin) {
		this.origin = origin;
	}

	@Override
	public String toString(){
		String s = "";

		if(clock != null){
			
			// Clock
			s += "[";
			for(int i = 0; i< clock.length; i++) {
				if(i==clock.length-1) {
					s+= String.format("%4d",clock[i]);
				} else {
					s+= String.format("%4d,",clock[i]);
				}
			}
			s+= "]";
			
			if(event != null)
				s += " " + event.name();
			
			if(job != null)
				s += " " + job.getId();
			
			if(origin != null)
				s += " " + origin.getHostName()+ ":"+origin.getPort();

		}

		return s;

	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

}

