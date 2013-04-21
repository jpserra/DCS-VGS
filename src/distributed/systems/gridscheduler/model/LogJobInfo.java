package distributed.systems.gridscheduler.model;

public class LogJobInfo {

	//private Job job;
	private long id;
	private boolean source;

	public LogJobInfo(long id, boolean source) {
		this.id = id;
		this.source = source;
	}

	public long getID() {return id;}
	public boolean isSource() {return source;}

}
