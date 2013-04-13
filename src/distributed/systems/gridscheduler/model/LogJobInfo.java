package distributed.systems.gridscheduler.model;

public class LogJobInfo {

	private Job job;
	private boolean source;

	public LogJobInfo(Job job, boolean source) {
		this.job = job;
		this.source = source;
	}

	public Job getJob() {return job;}
	public boolean isSource() {return source;}

}
