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
	RestartRM,


	// from GS to RM
	RestartRMAck,
	RequestLoad,
	AddJobAck,
	ResourceManagerJoinAck,
	JobArrivalAck,
	JobStartedAck,
	JobCompletedAck,
	SimulationOver,


	// both ways
	AddJob,
	
	// GS to GS
	RestartGS,
	RestartGSAck,
	GSRequestsGSList,
	GSLogJobArrival,
	GSLogJobArrivalAck,
	GSLogJobStarted,
	GSLogJobStartedAck,
	GSLogJobCompleted,
	GSLogJobCompletedAck,
	GSLogRestartGS,
	GSLogRestartGSAck,
	GSLogRestartRM,
	GSLogRestartRMAck,

	// GS to GS or GS to RM
	ReplyGSList

}
