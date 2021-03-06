package distributed.systems.gridscheduler.model;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.LogEntry;
import distributed.systems.core.LogEntryText;
import distributed.systems.core.LogEntryType;
import distributed.systems.core.LogManager;
import distributed.systems.core.Message;
import distributed.systems.core.SynchronizedClientSocket;
import distributed.systems.core.SynchronizedSocket;
import distributed.systems.core.VectorialClock;

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
public class ResourceManager implements INodeEventHandler, IMessageReceivedHandler, Runnable {

	private Cluster cluster;
	private Queue<Job> jobQueue;

	public static final int JOBID_MULTIPLICATION_FACTOR = 100000; 

	private String hostname;
	private int port;
	private int identifier;
	private VectorialClock vClock;
	private LogManager logger;
	private LogManager jobsLogger;

	private HashSet<Long> outsideJobsToExecute;
	private HashSet<Long> ownJobsToIgnore;

	private String logfilename = "";

	// timeout to recieve an ACK (response) message
	private static final int TIMEOUT = 4000;

	public static final int MAX_QUEUE_SIZE = 10; 

	// hostname and port (address) of the Grid Scheduler that this RM is supposed to use to connect in the first place.
	private String gsHostname = null;
	private int gsPort;

	//GS list with addresses and number of failures for each one
	private ConcurrentHashMap<InetSocketAddress, Integer> gsList;

	//Timers that control the job delegation for each job that was delegated.
	private ConcurrentHashMap<Long, Timer> jobTimers;
	private ConcurrentHashMap<Long, int[]> delegatedJobsClock;

	// polling frequency
	private long pollSleep = 100;
	private boolean running;
	private Thread pollingThread;
	private String jobsfilename;
	private int jobsDone;
	
	private boolean finished = false;

	//Getter's
	public HashSet<Long> getOutsideJobsToExecute() {return outsideJobsToExecute;}
	public HashSet<Long> getOwnJobsToIgnore() {return ownJobsToIgnore;}
	public ConcurrentHashMap<InetSocketAddress, Integer> getGsList() {return gsList;}

	private class ScheduledTask extends TimerTask implements Runnable {
		private IMessageReceivedHandler handler;
		private ControlMessage message;
		private InetSocketAddress destinationAddress;

		ScheduledTask(IMessageReceivedHandler handler, ControlMessage message, InetSocketAddress destinationAddress){
			this.handler = handler;
			this.message = message;
			this.destinationAddress = destinationAddress;
		}

		@Override
		public void run() {
			System.out.println("TIMEOUT JOB "+message.getJob().getId()+" @ "+System.currentTimeMillis());
			//System.out.println("TIMEOUT AddJob to "+destinationAddress.getHostName()+":"+destinationAddress.getPort());
			handler.onReadExceptionThrown(message, destinationAddress);
		}
	}

	/**
	 * Constructs a new ResourceManager object.
	 * <P> 
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>cluster</CODE> cannot be null
	 * </DL>
	 * @param cluster the cluster to wich this resource manager belongs.
	 */
	public ResourceManager(int id, int nEntities, Cluster cluster, boolean restart, String gsHostname,int gsPort)	{
		// preconditions
		assert(id >= 0);
		assert(nEntities > 0);
		assert(cluster != null);

		this.cluster = cluster;
		this.hostname = cluster.getName();
		this.port = cluster.getPort();
		this.identifier = id;
		this.gsHostname = gsHostname;
		this.gsPort = gsPort;

		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.vClock = new VectorialClock(nEntities);
		this.logfilename += hostname+":"+port+".log";

		this.jobsDone = 0;

		if(!restart) {
			File file = new File (logfilename);
			file.delete();
		}

		this.logger = new LogManager(logfilename);

		if(restart) {
			SynchronizedClientSocket syncClientSocket;
			logger.readOrderedLog();
			//Set the clock to continue
			int[][] orderedClocks = logger.getOrderedClocks();
			vClock.setIndexValue(id, (orderedClocks[orderedClocks.length-1][id]));
			System.out.println("INITIAL CLOCK AFTER RESTART: "+vClock.toString());
			// Save previous log
			logger.writeOrderedLogToTextfile("_restart");
			//
			getLogInformation();
			// IMPORTANT! Free up the memory
			logger.cleanupStructures();
			// Usage of log information is over...

			ControlMessage cMessage =  new ControlMessage(identifier, ControlMessageType.RestartRM, hostname, port);
			vClock.incrementClock(identifier);
			cMessage.setClock(vClock.getClock());
			LogEntry e = new LogEntry(cMessage);
			logger.writeAsText(e, true);
			// Only one GS in the list at the time.
			syncClientSocket = new SynchronizedClientSocket(cMessage, new InetSocketAddress(gsHostname, gsPort),this, TIMEOUT);
			syncClientSocket.sendMessage();
		}

		gsList = new ConcurrentHashMap<InetSocketAddress, Integer>();
		jobTimers = new ConcurrentHashMap<Long, Timer>();
		delegatedJobsClock = new ConcurrentHashMap<Long, int[]>();

		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();

	}

	private void getLogInformation() {
		// Get the log entries from the log
		LogEntryText[] orderedLog = logger.getLogEntriesOrdered();
		// 1. Query each entry to fill the structure
		LogEntryType evt = null;
		HashMap<Long, LogJobInfo> logInfo = new HashMap<Long, LogJobInfo>();
		ownJobsToIgnore = new HashSet<Long>();
		outsideJobsToExecute = new HashSet<Long>();
		LogJobInfo aux;
		for(LogEntryText e : orderedLog) {
			evt = e.getEvent();
			if(evt==LogEntryType.JOB_ARRIVAL_INT || evt==LogEntryType.JOB_ARRIVAL_EXT) {
				if(e.getJobID()/JOBID_MULTIPLICATION_FACTOR==identifier) {
					logInfo.put(e.getJobID(), new LogJobInfo(e.getJobID(), true));
				} else {
					logInfo.put(e.getJobID(), new LogJobInfo(e.getJobID(), false));
				}
			} else if (evt==LogEntryType.JOB_COMPLETED) {
				aux = logInfo.get(e.getJobID());
				if(aux != null) {
					if(aux.isSource()) {
						ownJobsToIgnore.add(aux.getID());
					}
				}
				logInfo.remove(e.getJobID());
			}
		}

		// Only the unfinished jobs are present now
		// 2. Let's define which ones are ours and which ones are from other entities
		for(Entry<Long, LogJobInfo> info : logInfo.entrySet()) {
			if (!info.getValue().isSource()) {
				outsideJobsToExecute.add(info.getKey());
			}
		}
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

		// if the jobqueue is full, offload the job to the grid scheduler
		if (jobQueue.size() >= cluster.getNodeCount() + MAX_QUEUE_SIZE) {

			int[] tempClock = null;
			InetSocketAddress address = getRandomGS();

			tempClock = vClock.incrementClock(identifier);

			ControlMessage controlMessage = new ControlMessage(identifier, ControlMessageType.AddJob, job, hostname, port);
			controlMessage.setClock(tempClock);
			SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(controlMessage, address, this, TIMEOUT);
			syncClientSocket.sendMessageWithoutResponse();

			// Schedule a timer to deal with the case where a AddJobAck message doesn't arrive in the specified timeout time.
			Timer t = new Timer();
			t.schedule(new ScheduledTask(this, controlMessage, address), TIMEOUT);
			jobTimers.put(job.getId(), t);
			delegatedJobsClock.put(job.getId(), tempClock);

			//System.out.println("JOB "+job.getId()+" sent to [GS "+address.getHostName()+":"+address.getPort()+"] @ "+System.currentTimeMillis());
			//System.out.println("[RM "+cluster.getID()+"] Job sent to [GS "+address.getHostName()+":"+address.getPort()+"]\n");

		} else { // otherwise store it in the local queue
			jobQueue.add(job);
			int[] tempClock = sendJobEvent(job, ControlMessageType.JobArrival);
			LogEntry e = null;
			//if(job.getOriginalRM().equals(new InetSocketAddress(hostname, port))) {
			e = new LogEntry(job, LogEntryType.JOB_ARRIVAL_INT, tempClock);
			//}
			logger.writeAsText(e,true);
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
	public synchronized void scheduleJobs() {
		Node freeNode;
		Job waitingJob;
		int[] tempClock;
		// while there are jobs to do and we have nodes available, assign the jobs to the free nodes
		while ( ((waitingJob = getWaitingJob()) != null) && ((freeNode = cluster.getFreeNode()) != null) ) {
			freeNode.startJob(waitingJob);
			tempClock = sendJobEvent(waitingJob,ControlMessageType.JobStarted);
			LogEntry e = new LogEntry(waitingJob, LogEntryType.JOB_STARTED, tempClock);
			logger.writeAsText(e,true);
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
		jobsDone++;
		int[] tempClock;
		// try to contact a GS in order to inform that a job was completed locally
		tempClock = sendJobEvent(job,ControlMessageType.JobCompleted);
		// write in the log that the job was executed
		LogEntry e = new LogEntry(job, LogEntryType.JOB_COMPLETED, tempClock);

		logger.writeAsText(e,true);
		// job finished, remove it from our pool
		jobQueue.remove(job);
	}

	/**
	 * @return the hostname of the grid scheduler this RM connected in the beginning.
	 */
	public String getGridSchedulerHostname() {
		return gsHostname;
	}

	/**
	 * @return the port of the grid scheduler this RM connected in the beggining.
	 */
	public int getGridSchedulerPort() {
		return gsPort;
	}

	/**
	 * @return the name of the log file.
	 */
	public String getLogFileName() {
		return logfilename;
	}
	/**
	 * Connect to a grid scheduler
	 * <p>
	 * pre: the parameter 'gridSchedulerURL' must not be null
	 * @param gridSchedulerHostname
	 * @param gridSchedulerPort
	 */
	public void connectToGridScheduler(String gridSchedulerHostname, int gridSchedulerPort) {

		// preconditions
		assert(gridSchedulerHostname != null) : "the parameter 'gridSchedulerHostname' cannot be null";
		assert(gridSchedulerPort > 0) : "the parameter 'gridSchedulerPort' cannot be less than or equal to 0";

		SynchronizedSocket syncSocket;
		syncSocket = new SynchronizedSocket(hostname, port);
		syncSocket.addMessageReceivedHandler(this);

		ControlMessage message = new ControlMessage(identifier, ControlMessageType.RMRequestsGSList, hostname, port);
		SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(message, new InetSocketAddress(gridSchedulerHostname, gridSchedulerPort), this, TIMEOUT);
		syncClientSocket.sendMessageInSameThread(message, true);

	}

	/**
	 * Message received handler
	 * <p>
	 * pre: parameter 'message' should be of type ControlMessage 
	 * pre: parameter 'message' should not be null 
	 * @param message a message
	 */
	public ControlMessage onMessageReceived(Message message) {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage)message;
		SynchronizedClientSocket syncClientSocket;

		if (controlMessage.getClock() != null) vClock.updateClock(controlMessage.getClock());

		// When Resource manager receives the list of all GS available from the GS that was given when this RM was initialized. The RM will try to join each of the GS
		if (controlMessage.getType() == ControlMessageType.ReplyGSList)
		{
			for (InetSocketAddress address:controlMessage.getGridSchedulersList()){
				gsList.put(address, 0);
				ControlMessage msg = new ControlMessage(identifier, ControlMessageType.ResourceManagerJoin, this.hostname, port);
				msg.setClock(vClock.getClock());
				syncClientSocket = new SynchronizedClientSocket(msg, address, this, TIMEOUT);
				syncClientSocket.sendMessage();
			}
		}

		// ResourceManager receives ack that the GS added them to theirs GS list.
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoinAck)
		{
			//Handled on exception if it doesn't receives ack
		}

		if (controlMessage.getType() == ControlMessageType.RequestLoad)
		{
			// RM updates the GS list.
			for(InetSocketAddress address : controlMessage.getGridSchedulersList()) {
				if(!gsList.containsKey(address)) {
					ControlMessage msg = new ControlMessage(identifier, ControlMessageType.ResourceManagerJoin, this.hostname, port);
					msg.setClock(vClock.getClock());
					syncClientSocket = new SynchronizedClientSocket(msg, address, this, TIMEOUT);
					syncClientSocket.sendMessage();
				}
				gsList.put(address, 0);
			}

			ControlMessage replyMessage = new ControlMessage(identifier, ControlMessageType.ReplyLoad,hostname,port);
			replyMessage.setLoad(((cluster.getNodeCount() + MAX_QUEUE_SIZE) - jobQueue.size()));
			//replyMessage.setLoad(((cluster.getNodeCount() + MAX_QUEUE_SIZE) - jobQueue.size())/gsList.size());
			return replyMessage;
		}

		// RM receives add Job from a GS
		if (controlMessage.getType() == ControlMessageType.AddJob)
		{
			//System.out.println("[RM "+cluster.getID()+"] Message received: " + controlMessage.getType()+" with JobID "+controlMessage.getJob().getId()+"\n");
			ControlMessage msg;
			LogEntry e = null;

			jobQueue.add(controlMessage.getJob());

			int[] tempVC = vClock.incrementClock(identifier);
			vClock.updateClock(controlMessage.getClock());

			// The Job came from one GS, so it is marked as an external one
			e = new LogEntry(controlMessage.getJob(), LogEntryType.JOB_ARRIVAL_EXT, tempVC);

			e.setOrigin(controlMessage.getInetAddress());

			//Now only sends message to the GS from where the message came from.
			msg = new ControlMessage(identifier, ControlMessageType.JobArrival, controlMessage.getJob(), this.hostname, this.port);
			msg.setClock(tempVC);

			logger.writeAsText(e,true);
			syncClientSocket = new SynchronizedClientSocket(msg, controlMessage.getInetAddress(), this, TIMEOUT);
			syncClientSocket.sendMessage();

			return null;

		}

		if (controlMessage.getType() == ControlMessageType.AddJobAck)
		{

			Timer t = jobTimers.remove(controlMessage.getJob().getId());
			if(t != null) {
				//System.out.println("AddJobAck --> Timer Cancelado! ID:"+controlMessage.getJob().getId());
				t.cancel();
				t.purge();
			}

			LogEntry e = new LogEntry(controlMessage.getJob(),LogEntryType.JOB_DELEGATED,delegatedJobsClock.remove(controlMessage.getJob().getId()));

			if(e.getClock()!=null) {
				logger.writeAsText(e,true);
			}

			return null;
		}

		if (controlMessage.getType() == ControlMessageType.JobArrivalAck ||
				controlMessage.getType() == ControlMessageType.JobCompletedAck ||
				controlMessage.getType() == ControlMessageType.JobStartedAck) {
			//vClock.updateClock(controlMessage.getClock());
		}

		if (controlMessage.getType() == ControlMessageType.SimulationOver) {
			if(!finished) {
				synchronized (this) {
					finished = true;
					System.out.println("Simulation is over:" + controlMessage.getUrl() + " " + controlMessage.getPort());
					System.out.println("Shutting down in 2 seconds...");
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("Jobs ran in this cluster: "+jobsDone);
					System.out.println("Writting log file in text format...");
					logger.readOrderedLog();
					logger.writeOrderedLogToTextfile("_final");
					logger.cleanupStructures();
					File f = new File(logger.getFilename());
					f.delete();
					System.out.println("Shutting down now!...");
					System.exit(0);
				}
			}
		}

		return null;

	}

	@Override
	public ControlMessage onConnectExceptionThrown(Message message,
			InetSocketAddress destinationAddress, boolean requiresRepsonse) {
		return onReadExceptionThrown(message,destinationAddress);
	}

	@Override
	public ControlMessage onWriteExceptionThrown(Message message,
			InetSocketAddress destinationAddress, boolean requiresRepsonse) {
		return onReadExceptionThrown(message,destinationAddress);
	}

	@Override
	public ControlMessage onReadExceptionThrown(Message message,
			InetSocketAddress destinationAddress) {
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage)message;

		// RM only receives messages from GS's.
		// When a GS has a certain amount of failures, it will be removed from the list.

		if (controlMessage.getType() == ControlMessageType.RestartRM) {
			return controlMessage;
		}

		checkGSFailures(destinationAddress);

		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {
			return controlMessage;
		}

		// RM keeps trying to get the GS list from the GS that was provided
		if (controlMessage.getType() == ControlMessageType.RMRequestsGSList) {
			return controlMessage;
		}

		// if jobAdd fails it will try to add it to the queue again
		if (controlMessage.getType() == ControlMessageType.AddJob) {
			LogEntry e = new LogEntry(controlMessage.getJob(),LogEntryType.JOB_DELEGATED_FAIL,delegatedJobsClock.remove(controlMessage.getJob().getId()));
			logger.writeAsText(e,true);
			Timer t = jobTimers.remove(controlMessage.getJob().getId());
			if (t != null) {
				//System.out.println("AddJob FAILED --> Timer Cancelado! ID:"+controlMessage.getJob().getId());
				t.cancel();
				t.purge();
			}
			//Adds job again to RM, if it has free space it can execute it, or it can send it again to a GS
			addJob(controlMessage.getJob());
		}

		if (controlMessage.getType() == ControlMessageType.JobArrival ||
				controlMessage.getType() == ControlMessageType.JobStarted ||
				controlMessage.getType() == ControlMessageType.JobCompleted) {
			// Send log message to a randomly chosen GS.
			InetSocketAddress newAddress = getRandomGS();
			//System.out.println("[RM "+cluster.getID()+"] "+controlMessage.getType().name()+" FAILED! to [GS "+destinationAddress.getHostName()+":"+destinationAddress.getPort()+"]\n Sending to "+newAddress.getHostName()+":"+newAddress.getPort()+" now...");
			SynchronizedClientSocket s = new SynchronizedClientSocket(controlMessage, newAddress ,this, TIMEOUT);
			s.sendMessage();
		}
		return null;
	}

	private synchronized boolean checkGSFailures(InetSocketAddress destinationAddress) {
		Integer failures = gsList.get(destinationAddress);
		if(failures != null) {
			if (failures > 1) {
				gsList.remove(destinationAddress);
				return false;
			}
			else {
				gsList.replace(destinationAddress,failures+1);
			}
		}
		return false;
	}

	public void run() {
		while (running) {
			scheduleJobs();
			// sleep
			try
			{
				Thread.sleep(pollSleep);
			} catch (InterruptedException ex) {
				assert(false) : "Grid scheduler runtread was interrupted";
			}
		}
	}

	/**
	 * Stop the polling thread. This has to be called explicitly to make sure the program 
	 * terminates cleanly.
	 *
	 */
	public void stopPollThread() {
		running = false;
		try {
			pollingThread.join();
		} catch (InterruptedException ex) {
			assert(false) : "Grid scheduler stopPollThread was interrupted";
		}

	}

	/**
	 * Randomly selects one GS from the poll of GS's
	 * @return the address of the choosen GS
	 */
	private InetSocketAddress getRandomGS() {
		return (InetSocketAddress)gsList.keySet().toArray()[(int)(Math.random() * ((gsList.size()-1) + 1))];
	}

	/**
	 * Sends a job event (Arrival, Started or Comleted) to a randomly choosen GS.
	 * @param job
	 * @param messageType
	 */
	private synchronized int[] sendJobEvent(Job job, ControlMessageType messageType) {
		ControlMessage msg;
		int[] tempClock;
		msg = new ControlMessage(identifier, messageType, job, this.hostname, this.port);
		tempClock = vClock.incrementClock(this.identifier);
		msg.setClock(tempClock);
		SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(msg, getRandomGS(), this, TIMEOUT);
		syncClientSocket.sendMessage();
		return tempClock;
	}

}
