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


	// from GS to RM
	RequestLoad,
	AddJobAck,
	ResourceManagerJoinAck,

	

	// both ways
	AddJob,
	
	// GS to GS
	GSRequestsGSList,

	// GS to GS or GS to RM
	ReplyGSList


}
