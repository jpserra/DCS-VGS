package distributed.systems.gridscheduler.model;

/**
 * 
 * The NodeStatus enumeration expresses that state of a node. It can be available, busy or down. 
 * When a node is <i>idle</i>, there is no job running at that node. If there is a job running at the node, its
 * status is <i>busy</i>, indicating that it cannot accept another job. Finally, a node can also be <i>down</i> due
 * to technical failure.    
 * 
 * @author Niels brouwers
 * @see Node
 *
 */
public enum NodeStatus {

	Idle,
	Busy,
	Down

}
