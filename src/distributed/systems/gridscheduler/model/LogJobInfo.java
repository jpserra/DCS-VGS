package distributed.systems.gridscheduler.model;

public class LogJobInfo {

	
	private long id;
	private boolean source;

	public LogJobInfo(long id, boolean source) {
		this.id = id;
		this.source = source;
	}

	public long getId() {
		return id;
	}

	public void setSource(boolean source) {
		this.source = source;
	}

}
