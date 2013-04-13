package distributed.systems.gridscheduler.model;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.LogEntry;
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

	private String hostname;
	private int port;
	private int identifier;
	private VectorialClock vClock;
	private LogManager logger;

	private HashMap<Long, Job> outsideJobsToExecute;
	private HashMap<Long, Job> ownJobsToIgnore;

	private String logfilename = "";

	// timeout to recieve an ACK (response) message
	private final int timeout = 2000;

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

	//Getter's
	public HashMap<Long, Job> getOutsideJobsToExecute() {return outsideJobsToExecute;}
	public HashMap<Long, Job> getOwnJobsToIgnore() {return ownJobsToIgnore;}
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
	public ResourceManager(int id, int nEntities, Cluster cluster, boolean restart)	{
		// preconditions
		assert(id >= 0);
		assert(nEntities > 0);
		assert(cluster != null);

		this.cluster = cluster;
		this.hostname = cluster.getName();
		this.port = cluster.getPort();
		this.identifier = id;

		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.vClock = new VectorialClock(nEntities);
		this.logfilename += hostname+":"+port+".log";
		this.logger = new LogManager(logfilename);

		if(restart) {
			LogEntry[] orderedLog = logger.readOrderedLog();
			//Set the clock to the value where it stopped.
			vClock.setIndexValue(id, orderedLog[orderedLog.length-1].getClock()[id]);
			System.out.println("INITIAL CLOCK AFTER RESTART: "+vClock.toString());
			logger.writeOrderedRestartToTextfile();
			getLogInformation(orderedLog);
		}
		else {
			File file = new File (logfilename);
			file.delete();
		}

		gsList = new ConcurrentHashMap<InetSocketAddress, Integer>();
		jobTimers = new ConcurrentHashMap<Long, Timer>();
		delegatedJobsClock = new ConcurrentHashMap<Long, int[]>();

		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();

	}

	private void getLogInformation(LogEntry[] orderedLog) {
		// 1. Query each entry to fill the structure
		LogEntryType evt = null;
		HashMap<Long, LogJobInfo> logInfo = new HashMap<Long, LogJobInfo>();
		ownJobsToIgnore = new HashMap<Long, Job>();
		outsideJobsToExecute = new HashMap<Long, Job>();
		LogJobInfo aux;
		for(LogEntry e : orderedLog) {
			evt = e.getEvent();
			if(evt==LogEntryType.JOB_ARRIVAL_INT) {
				logInfo.put(e.getJob().getId(), new LogJobInfo(e.getJob(), true));
			} else if (evt==LogEntryType.JOB_ARRIVAL_EXT) {
				logInfo.put(e.getJob().getId(), new LogJobInfo(e.getJob(), false));
			} else if (evt==LogEntryType.JOB_COMPLETED) {
				aux = logInfo.get(e.getJob().getId());
				if(aux != null) {
					if(aux.isSource()) {
						ownJobsToIgnore.put(aux.getJob().getId(), aux.getJob());
					}
				}
				logInfo.remove(e.getJob().getId());
			}
		}

		// Only the unfinished jobs are present now
		// 2. Let's define which ones are ours and which ones are from other entities
		for(Entry<Long, LogJobInfo> info : logInfo.entrySet()) {
			if (!info.getValue().isSource()) {
				outsideJobsToExecute.put(info.getKey(),info.getValue().getJob());
			}
		}
		System.out.println("Own Jobs Finished: "+ownJobsToIgnore.keySet());
		System.out.println("Outside Jobs to Execute: "+outsideJobsToExecute.keySet());
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

			tempClock = vClock.incrementClock(identifier).clone();

			ControlMessage controlMessage = new ControlMessage(identifier, ControlMessageType.AddJob, job, hostname, port);
			controlMessage.setClock(tempClock);
			SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(controlMessage, address, this, timeout);
			syncClientSocket.sendMessageWithoutResponse();

			// Schedule a timer to deal with the case where a AddJobAck message doesn't arrive in the specified timeout time.
			Timer t = new Timer();
			t.schedule(new ScheduledTask(this, controlMessage, address), timeout);
			jobTimers.put(job.getId(), t);

			delegatedJobsClock.put(job.getId(), tempClock);

			//System.out.println("JOB "+job.getId()+" sent to [GS "+address.getHostName()+":"+address.getPort()+"] @ "+System.currentTimeMillis());
			//System.out.println("[RM "+cluster.getID()+"] Job sent to [GS "+address.getHostName()+":"+address.getPort()+"]\n");

		} else { // otherwise store it in the local queue
			if(jobQueue.add(job))
				System.out.println("JOB "+job.getId()+" added to local queue...");
			int[] tempClock = sendJobEvent(job, ControlMessageType.JobArrival);
			LogEntry e = null;
			if(job.getOriginalRM().equals(new InetSocketAddress(hostname, port))) {
				e = new LogEntry(job, LogEntryType.JOB_ARRIVAL_INT, tempClock);
			} else
				System.out.println("##################################### ERRO #############################");
			
			logger.writeToBinary(e,true);
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
		Node freeNode;
		Job waitingJob;
		int[] tempClock;
		// while there are jobs to do and we have nodes available, assign the jobs to the free nodes
		while ( ((waitingJob = getWaitingJob()) != null) && ((freeNode = cluster.getFreeNode()) != null) ) {
			freeNode.startJob(waitingJob);
			tempClock = sendJobEvent(waitingJob,ControlMessageType.JobStarted);
			LogEntry e = new LogEntry(waitingJob, LogEntryType.JOB_STARTED, tempClock);
			
			logger.writeToBinary(e,true);

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
		int[] tempClock;
		// try to contact a GS in order to inform that a job was completed locally
		tempClock = sendJobEvent(job,ControlMessageType.JobCompleted);
		// write in the log that the job was executed
		LogEntry e = new LogEntry(job, LogEntryType.JOB_COMPLETED, tempClock);
		
		logger.writeToBinary(e,true);
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

		this.gsHostname = gridSchedulerHostname;
		this.gsPort = gridSchedulerPort;

		SynchronizedSocket syncSocket;
		syncSocket = new SynchronizedSocket(hostname, port);
		syncSocket.addMessageReceivedHandler(this);

		ControlMessage message = new ControlMessage(identifier, ControlMessageType.RMRequestsGSList, hostname, port);
		SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(message, new InetSocketAddress(gridSchedulerHostname, gridSchedulerPort), this, timeout);
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

		if(controlMessage.getType() != ControlMessageType.RequestLoad) {
			//System.out.println("[RM "+cluster.getID()+"] Message received: " + controlMessage.getType()+"\n");
		}

		// When Resource manager receives the list of all GS available from the GS that was given when this RM was initialized. The RM will try to join each of the GS
		if (controlMessage.getType() == ControlMessageType.ReplyGSList)
		{
			for (InetSocketAddress address:controlMessage.getGridSchedulersList()){
				gsList.put(address, 0);
				ControlMessage msg = new ControlMessage(identifier, ControlMessageType.ResourceManagerJoin, this.hostname, port);
				msg.setClock(vClock.getClock());
				syncClientSocket = new SynchronizedClientSocket(msg, address, this, timeout);
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
					syncClientSocket = new SynchronizedClientSocket(msg, address, this, timeout);
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
			//Now only sends message to the GS from where the message came from.

			int[] tempVC = vClock.incrementClock(identifier);
			vClock.updateClock(controlMessage.getClock());
			
			if(controlMessage.getJob().getId()/100000==identifier)
				e = new LogEntry(controlMessage.getJob(), LogEntryType.JOB_ARRIVAL_INT, tempVC);
			else
				e = new LogEntry(controlMessage.getJob(), LogEntryType.JOB_ARRIVAL_EXT, tempVC);
			e.setOrigin(controlMessage.getInetAddress());
			
			msg = new ControlMessage(identifier, ControlMessageType.JobArrival, controlMessage.getJob(), this.hostname, this.port);
			msg.setClock(tempVC);

			
			logger.writeToBinary(e,true);
			syncClientSocket = new SynchronizedClientSocket(msg, controlMessage.getInetAddress(), this, timeout);
			syncClientSocket.sendMessage();

			scheduleJobs();

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
			
			if(e.getClock()!=null)
				logger.writeToBinary(e,true);

			return null;
		}

		if (controlMessage.getType() == ControlMessageType.JobArrivalAck ||
				controlMessage.getType() == ControlMessageType.JobCompletedAck ||
				controlMessage.getType() == ControlMessageType.JobStartedAck) {
			//vClock.updateClock(controlMessage.getClock());
		}

		if (controlMessage.getType() == ControlMessageType.SimulationOver) {
			System.out.println("SIMULATION OVER:" + controlMessage.getUrl() + " " + controlMessage.getPort());
			System.out.println("Shutting down...");
			logger.writeToTextfile();
			logger.writeOrderedToTextfile();
			System.exit(0);
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
		checkGSFailures(destinationAddress);

		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {
			return controlMessage;
		}

		// RM keeps trying to get the GS list from the GS that was provided
		else if (controlMessage.getType() == ControlMessageType.RMRequestsGSList) {
			return controlMessage;
		}

		// if jobAdd fails it will add to the jobQueue again
		else if (controlMessage.getType() == ControlMessageType.AddJob) {
			LogEntry e = new LogEntry(controlMessage.getJob(),LogEntryType.JOB_DELEGATED_FAIL,delegatedJobsClock.remove(controlMessage.getJob().getId()));
			
			logger.writeToBinary(e,true);
			Timer t = jobTimers.remove(controlMessage.getJob().getId());
			if (t != null) {
				//System.out.println("AddJob FAILED --> Timer Cancelado! ID:"+controlMessage.getJob().getId());
				t.cancel();
				t.purge();
			}
			//Adds job again to RM, if it has free space it can execute it, or it can send it again to a GS
			addJob(controlMessage.getJob());
		}

		else if (controlMessage.getType() == ControlMessageType.JobArrival ||
				controlMessage.getType() == ControlMessageType.JobStarted ||
				controlMessage.getType() == ControlMessageType.JobCompleted) {
			// Send log message to a randomly chosen GS.
			InetSocketAddress newAddress = getRandomGS();
			//System.out.println("[RM "+cluster.getID()+"] "+controlMessage.getType().name()+" FAILED! to [GS "+destinationAddress.getHostName()+":"+destinationAddress.getPort()+"]\n Sending to "+newAddress.getHostName()+":"+newAddress.getPort()+" now...");
			SynchronizedClientSocket s = new SynchronizedClientSocket(controlMessage, newAddress ,this, timeout);
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
		SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(msg, getRandomGS(), this, timeout);
		syncClientSocket.sendMessage();
		return tempClock;
	}

	public LogEntry[] getFullLog(){

		LogEntry[] log = logger.readOrderedLog();
		try {

			File file = new File(logfilename+"_readable");

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			for(LogEntry m : log) {
				bw.write(m.toString() + "\n");
			}

			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		return log;

	}

}
