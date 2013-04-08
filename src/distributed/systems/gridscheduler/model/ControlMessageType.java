package distributed.systems.gridscheduler.model;

/**
 * 
 * Different types of control messages. Feel free to add new message types if you need any. 
 * 
 * @author Niels Brouwers
 *
 */
public enum ControlMessageType {

	// from RM to GS
	ResourceManagerJoin,
	ReplyLoad,
	RMRequestsGSList,
	JobArrival,
	JobStarted,
	JobCompleted,

	// from GS to RM
	RequestLoad,
	AddJobAck,
	ResourceManagerJoinAck,
	JobArrivalAck,
	JobStartedAck,
	JobCompletedAck,
	SimulationOver,
	Restart,
	RestartAck,

	// both ways
	AddJob,
	
	// GS to GS
	GSRequestsGSList,
	GSSendLogEntry,
	GSLogEntryAck,
	GSLogJobArrival,
	GSLogJobArrivalAck,
	GSLogJobStarted,
	GSLogJobStartedAck,
	GSLogJobCompleted,
	GSLogJobCompletedAck,
	GSLogRestart,
	GSLogRestartAck,

	// GS to GS or GS to RM
	ReplyGSList

}
