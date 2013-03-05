package distributed.systems.gridscheduler.model;

/**
 * 
 * The possible states of a job. When a job is first created, it is initialized to the <i>Waiting</i>
 * state. When it is assigned to a node and execution is started, its state changes to <i>Running</i>.
 * Finally, when a job is finished it moves into the <i>Done</i> state.   
 * 
 * @author Niels Brouwers
 *
 */
public enum JobStatus {

	/**
	 * Indicates that the job has been freshly created and is waiting to be executed. 
	 */
	Waiting,

	/**
	 * Indicates that the job is running on some node. 
	 */
	Running,

	/**
	 * Indicates that the job is finished. 
	 */
	Done

}
