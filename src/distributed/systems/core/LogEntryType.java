package distributed.systems.core;

import java.io.Serializable;

/**
 * 
 * Different types of control messages. Feel free to add new message types if you need any. 
 * 
 * @author Niels Brouwers
 *
 */
public enum LogEntryType implements Serializable {

	// used by RM only
	JOB_ARRIVAL_INT,
	JOB_ARRIVAL_EXT,
	JOB_DELEGATED,
	JOB_DELEGATED_FAIL,
	
	// used by GS only
	JOB_ARRIVAL,
	RESTART_GS,
	
	// used by both
	JOB_STARTED,
	JOB_COMPLETED,
	RESTART_RM,
	
	// unknown log
	UNKNOWN

}
