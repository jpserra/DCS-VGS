package distributed.systems.gridscheduler.model;

/**
 * This class represents a job that can be executed on a grid. 
 * 
 * @author Niels Brouwers
 *
 */
public class Job {
	private long duration;
	private JobStatus status;
	private long id;

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
		// Preconditions
		assert(duration > 0) : "parameter 'duration' should be > 0";

		this.duration = duration;
		this.status = JobStatus.Waiting;
		this.id = id; 
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
	public String toString() {
		return "Job {ID = " + id + "}";
	}

}
