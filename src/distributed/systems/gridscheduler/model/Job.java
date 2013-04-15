package distributed.systems.gridscheduler.model;

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * This class represents a job that can be executed on a grid. 
 * 
 * @author Niels Brouwers
 *
 */
public class Job implements Serializable{

	private static final long serialVersionUID = 877471067429263379L;
	private long id;
	private long duration;
	private JobStatus status;
	private InetSocketAddress originalRM;
	private InetSocketAddress responsibleRM;
	
	public InetSocketAddress getResponsibleRM() {
		return responsibleRM;
	}

	public void setResponsibleRM(InetSocketAddress responsibleRM) {
		this.responsibleRM = responsibleRM;
	}

	/**
	 * Constructs a new Job object with a certain duration and id. The id has to be unique
	 * within the distributed system to avoid collisions.
	 * <P>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>duration</CODE> should be positive
	 * </DL> 
	 * @param duration job duration in milliseconds 
	 * @param id job ID
	 */
	public Job(long duration, long id) {
		assert(duration > 0) : "parameter 'duration' should be > 0";
		this.duration = duration;
		this.status = JobStatus.Waiting;
		this.id = id; 
		this.originalRM = null;
	}

	/**
	 * Returns the duration of this job. 
	 * @return the total duration of this job
	 */
	public double getDuration() {
		return duration;
	}

	/**
	 * Returns the status of this job.
	 * @return the status of this job
	 */
	public JobStatus getStatus() {
		return status;
	}

	/**
	 * Sets the status of this job.
	 * @param status the new status of this job
	 */
	public void setStatus(JobStatus status) {
		this.status = status;
	}

	/**
	 * The message ID is a unique identifier for a message. 
	 * @return the message ID
	 */
	public long getId() {
		return id;
	}

	/**
	 * @return a string representation of this job object
	 */
	@Override
	public String toString() {
		return "Job {ID = " + id +"}";
	}

	public InetSocketAddress getOriginalRM() {
		return originalRM;
	}

	public void setOriginalRM(InetSocketAddress originalRM) {
		this.originalRM = originalRM;
	}

}
