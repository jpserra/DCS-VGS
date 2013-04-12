package distributed.systems.gridscheduler.model;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import distributed.systems.core.LogEntry;
import distributed.systems.core.Message;

/**
 * 
 * Class that represents the messages being exchanged in the VGS. It has some members to
 * facilitate the passing of common arguments. Feel free to expand it and adapt it to your 
 * needs. 
 * 
 * @author Niels Brouwers
 *
 */
public class ControlMessage extends Message {

	/**
	 * Generated serial version UID
	 */
	private static final long serialVersionUID = 1453428681740343634L;

	private final ControlMessageType type;
	private int id;
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}


	private String url;
	private int port;
	private LogEntry logEntry;
	private Job job;
	private int load;
	private Set<InetSocketAddress> gridSchedulersList;
	private int[] clock;

	/**
	 * Constructs a new ControlMessage object
	 * @param type the type of control message
	 */
	public ControlMessage(int id, ControlMessageType type) {
		this.type = type;
	}

	public ControlMessage(int id, ControlMessageType type, String url, int port) {
		this.type = type;
		this.url = url;
		this.port = port;
	}
	
	public ControlMessage(int id, ControlMessageType type, Job job, String url, int port) {
		this.type = type;
		this.job = job;
		this.url = url;
		this.port = port;
	}
	
	public ControlMessage(int id, ControlMessageType type, String url, int port, int[] clock) {
		this.type = type;
		this.url = url;
		this.port = port;
		this.clock = clock;
	}
	
	public ControlMessage(int id, ControlMessageType type, Job job, String url, int port, int[] clock) {
		this.type = type;
		this.job = job;
		this.url = url;
		this.port = port;
		this.clock = clock;
	}

	public ControlMessage(int id, ControlMessageType type, LogEntry logEntry, String url, int port) {
		this.type = type;
		this.logEntry = logEntry;
		this.url = url;
		this.port = port;

	}


	/**
	 * @return the job
	 */
	public Job getJob() {
		return job;
	}

	/**
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>job</CODE> cannot be null
	 * </DL>
	 * @param job the job to set
	 */
	public void setJob(Job job) {
		assert(job != null) : "parameter 'job' cannot be null";
		this.job = job;
	}

	/**
	 * @return the load
	 */
	public int getLoad() {
		return load;
	}

	/**
	 * @param load the load to set
	 */
	public void setLoad(int load) {
		this.load = load;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the url
	 */
	public int getPort() {
		return port;
	}

	public InetSocketAddress getInetAddress(){
		return new InetSocketAddress(url, port);
	}

	/**
	 * @param url the url to set
	 */
	public void setHostname(String url) {
		this.url = url;
	}


	/**
	 * @return the type
	 */
	public ControlMessageType getType() {
		return type;
	}

	public Set<InetSocketAddress> getGridSchedulersList() {
		return gridSchedulersList;
	}

	public void setGridSchedulersList(Set<InetSocketAddress> gridSchedulersList) {
		this.gridSchedulersList = new HashSet<InetSocketAddress>(gridSchedulersList);
	}

	public LogEntry getLogEntry() {
		return logEntry;
	}

	public void setLogEntry(LogEntry logEntry) {
		this.logEntry = logEntry;
	}

	public int[] getClock() {
		return clock;
	}

	public void setClock(int[] clock) {
		this.clock = clock;
	}


	@Override
	public String toString(){
		String s = "[";

		if(clock != null){
			s += "VecClock: [ ";

			for(int i = 0; i< clock.length; i++)
				s+= clock[i] + ", ";

			s+= " ]";
		}
		if(this.url != null)
			s += "Origin: " + url+ ":"+port + " | ";

		//		if(id != null)
		//			s += "ID: " + id + " | ";

		if(this.type != null)
			s += "Event: " + this.type.toString() + " | "; 

		if(job != null)
			s += "Job: " + job.toString() + " | ";


		s+= " ]";
		return s;

	}
}
