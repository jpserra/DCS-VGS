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

	// from GS to RM
	RequestLoad,

	// both ways
	AddJob

}
