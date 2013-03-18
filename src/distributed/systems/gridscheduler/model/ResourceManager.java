package distributed.systems.gridscheduler.model;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;
import distributed.systems.core.SynchronizedSocket;
//import distributed.systems.example.LocalSocket;

/**
 * This class represents a resource manager in the VGS. It is a component of a cluster, 
 * and schedulers jobs to nodes on behalf of that cluster. It will offload jobs to the grid
 * scheduler if it has more jobs waiting in the queue than a certain amount.
 * 
 * The <i>jobQueueSize</i> is a variable that indicates the cutoff point. If there are more
 * jobs waiting for completion (including the ones that are running at one of the nodes)
 * than this variable, jobs are sent to the grid scheduler instead. This variable is currently
 * defaulted to [number of nodes] + MAX_QUEUE_SIZE. This means there can be at most MAX_QUEUE_SIZE jobs waiting 
 * locally for completion. 
 * 
 * Of course, this scheme is totally open to revision.
 * 
 * @author Niels Brouwers, Boaz Pat-El
 *
 */
public class ResourceManager implements INodeEventHandler, IMessageReceivedHandler {
	
	private Cluster cluster;
	private Queue<Job> jobQueue;
	private String socketURL;
	private int socketPort;

	//	private int jobQueueSize;
	public static final int MAX_QUEUE_SIZE = 10; 

	// Scheduler url
	private String gridSchedulerURL = null;
	private int gridSchedulerPort;

	private SynchronizedSocket syncSocket;

	private Set<InetSocketAddress> gsList;

	/**
	 * Constructs a new ResourceManager object.
	 * <P> 
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>cluster</CODE> cannot be null
	 * </DL>
	 * @param cluster the cluster to wich this resource manager belongs.
	 */
	public ResourceManager(Cluster cluster)	{
		// preconditions
		assert(cluster != null);

		this.jobQueue = new ConcurrentLinkedQueue<Job>();

		this.cluster = cluster;
		this.socketURL = cluster.getName();
		this.socketPort = cluster.getPort();
		// Number of jobs in the queue must be larger than the number of nodes, because
		// jobs are kept in queue until finished. The queue is a bit larger than the 
		// number of nodes for efficiency reasons - when there are only a few more jobs than
		// nodes we can assume a node will become available soon to handle that job.
//		jobQueueSize = cluster.getNodeCount() + MAX_QUEUE_SIZE;

		/*
		//LocalSocket lSocket = new LocalSocket();
		Socket lSocket = new Socket();
		socket = new SynchronizedSocket(lSocket);
		//socket.register(socketURL);

		socket.addMessageReceivedHandler(this);
		*/
	}

	/**
	 * Add a job to the resource manager. If there is a free node in the cluster the job will be
	 * scheduled onto that Node immediately. If all nodes are busy the job will be put into a local
	 * queue. If the local queue is full, the job will be offloaded to the grid scheduler.
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>job</CODE> cannot be null
	 * <DD>a grid scheduler url has to be set for this rm before calling this function (the RM has to be
	 * connected to a grid scheduler)
	 * </DL>
	 * @param job the Job to run
	 */
	public void addJob(Job job) {
		// check preconditions
		assert(job != null) : "the parameter 'job' cannot be null";
		assert(gridSchedulerURL != null) : "No grid scheduler URL has been set for this resource manager";
		
		int index;
		InetSocketAddress address;

		// if the jobqueue is full, offload the job to the grid scheduler
		if (jobQueue.size() >= cluster.getNodeCount() + MAX_QUEUE_SIZE) {

			ControlMessage controlMessage = new ControlMessage(ControlMessageType.AddJob);
			controlMessage.setJob(job);
			controlMessage.setUrl(this.socketURL);
			controlMessage.setPort(this.socketPort);
			
			//TODO Add job is always adding to the same gridScheduler
			//syncSocket.sendMessage(controlMessage, new InetSocketAddress(gridSchedulerURL, gridSchedulerPort) );
			
			index = (int)(Math.random() * ((gsList.size()-1) + 1));
			address = (InetSocketAddress)gsList.toArray()[index];
			
			syncSocket.sendMessage(controlMessage, address);
			
			System.out.println("[RM "+cluster.getID()+"] Job sent to [GS "+address.getHostString()+":"+address.getPort()+"]\n");

			// otherwise store it in the local queue
		} else {
			jobQueue.add(job);
			scheduleJobs();
		}

	}

	/**
	 * Tries to find a waiting job in the jobqueue.
	 * @return
	 */
	public Job getWaitingJob() {
		// find a waiting job
		for (Job job : jobQueue) 
			if (job.getStatus() == JobStatus.Waiting) 
				return job;

		// no waiting jobs found, return null
		return null;
	}

	/**
	 * Tries to schedule jobs in the jobqueue to free nodes. 
	 */
	public void scheduleJobs() {
		// while there are jobs to do and we have nodes available, assign the jobs to the 
		// free nodes
		Node freeNode;
		Job waitingJob;

		while ( ((waitingJob = getWaitingJob()) != null) && ((freeNode = cluster.getFreeNode()) != null) ) {
			freeNode.startJob(waitingJob);
		}

	}

	/**
	 * Called when a job is finished
	 * <p>
	 * pre: parameter 'job' cannot be null
	 */
	public void jobDone(Job job) {
		// preconditions
		assert(job != null) : "parameter 'job' cannot be null";

		// job finished, remove it from our pool
		jobQueue.remove(job);
	}

	/**
	 * @return the url of the grid scheduler this RM is connected to 
	 */
	public String getGridSchedulerURL() {
		return gridSchedulerURL;
	}
	
	public int getGridSchedulerPort() {
		return gridSchedulerPort;
	}

	/**
	 * Connect to a grid scheduler
	 * <p>
	 * pre: the parameter 'gridSchedulerURL' must not be null
	 * @param gridSchedulerURL
	 */
	public void connectToGridScheduler(String gridSchedulerURL, int gridSchedulerPort) {

		// preconditions
		assert(gridSchedulerURL != null) : "the parameter 'gridSchedulerURL' cannot be null"; 

		this.gridSchedulerURL = gridSchedulerURL;
		this.gridSchedulerPort = gridSchedulerPort;
		
		syncSocket = new SynchronizedSocket(socketURL, socketPort);
		syncSocket.addMessageReceivedHandler(this);
		
		ControlMessage message = new ControlMessage(ControlMessageType.RMRequestsGSList);
		message.setUrl(socketURL);
		message.setPort(socketPort);
		syncSocket.sendMessage(message, new InetSocketAddress(gridSchedulerURL, gridSchedulerPort));				

		/*
		ControlMessage message = new ControlMessage(ControlMessageType.ResourceManagerJoin);
		message.setUrl(socketURL);
		message.setPort(socketPort);
		syncSocket.sendMessage(message, new InetSocketAddress(gridSchedulerURL, gridSchedulerPort));				
*/
	}

	/**
	 * Message received handler
	 * <p>
	 * pre: parameter 'message' should be of type ControlMessage 
	 * pre: parameter 'message' should not be null 
	 * @param message a message
	 */
	public void onMessageReceived(Message message) {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage)message;

		//TODO Tirar o IF se for para ver os prints todos...
		if(controlMessage.getType() != ControlMessageType.RequestLoad) {
			System.out.println("[RM "+cluster.getID()+"] Message received: " + controlMessage.getType()+"\n");
		}

		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob)
		{
			jobQueue.add(controlMessage.getJob());
			scheduleJobs();
		}

		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.RequestLoad)
		{
			ControlMessage replyMessage = new ControlMessage(ControlMessageType.ReplyLoad);
			replyMessage.setUrl(socketURL);
			replyMessage.setPort(socketPort);
			replyMessage.setLoad(jobQueue.size());
			syncSocket.sendMessage(replyMessage, controlMessage.getInetAddress());				
		}
		

		if (controlMessage.getType() == ControlMessageType.ReplyGSList)
		{
			gsList = controlMessage.getGridSchedulersList();
			
			System.out.println("GSList:" + gsList);
			for(InetSocketAddress address : gsList) {
				ControlMessage msg = new ControlMessage(ControlMessageType.ResourceManagerJoin);
				msg.setUrl(this.socketURL);
				msg.setPort(socketPort);
				syncSocket.sendMessage(msg, address);				
			}

		}

	}

}
