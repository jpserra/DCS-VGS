package distributed.systems.gridscheduler.model;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.sound.midi.ControllerEventListener;

import distributed.systems.core.IMessageReceivedHandler;
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
	private String socketURL;
	private int socketPort;

	private int identifier;
	private int nEntities;
	private VectorialClock vClock;


	private String logfilename = "";

	private final long timeOut = 1000;

	//	private int jobQueueSize;
	public static final int MAX_QUEUE_SIZE = 10; 

	// Scheduler url
	private String gridSchedulerURL = null;
	private int gridSchedulerPort;

	private ConcurrentHashMap<InetSocketAddress, Integer> gsList; //Gs lists addresses and number of failures

	private ConcurrentHashMap<Long, Timer> jobTimers; 
	//private ConcurrentHashMap<InetSocketAddress> gsList;

	// polling frequency, 1hz
	private long pollSleep = 100;
	private boolean running;
	private Thread pollingThread;


	/**
	 * Constructs a new ResourceManager object.
	 * <P> 
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>cluster</CODE> cannot be null
	 * </DL>
	 * @param cluster the cluster to wich this resource manager belongs.
	 */
	public ResourceManager(int id, int nEntities, Cluster cluster)	{
		// preconditions
		assert(cluster != null);

		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.cluster = cluster;
		this.socketURL = cluster.getName();
		this.socketPort = cluster.getPort();
		this.identifier = id;
		this.nEntities = nEntities;
		vClock = new VectorialClock(nEntities);
		logfilename += socketURL+":"+socketPort+".log";

		File file = new File (logfilename);
		file.delete();

		gsList = new ConcurrentHashMap<InetSocketAddress, Integer>();

		jobTimers = new ConcurrentHashMap<Long, Timer>();


		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();


	}


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
			// TODO Auto-generated method stub
			System.out.println("TIME OUT!");
			handler.onExceptionThrown(message, destinationAddress);
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
		assert(gridSchedulerURL != null) : "No grid scheduler URL has been set for this resource manager";

		InetSocketAddress address;
		job.setOriginalRM(new InetSocketAddress(socketURL, socketPort));
		// if the jobqueue is full, offload the job to the grid scheduler
		if (jobQueue.size() >= cluster.getNodeCount() + MAX_QUEUE_SIZE) {

			address = getRandomGS();

			ControlMessage controlMessage = new ControlMessage(ControlMessageType.AddJob, job, socketURL, socketPort);
			controlMessage.setClock(vClock.getClock());
			SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(controlMessage, address, this);
			syncClientSocket.sendMessageWithoutResponse();

			Timer t = new Timer();
			t.schedule(new ScheduledTask(this, controlMessage, address), timeOut);
			jobTimers.put(job.getId(), t);

			System.out.println("[RM "+cluster.getID()+"] Job sent to [GS "+address.getHostString()+":"+address.getPort()+"]\n");

			// otherwise store it in the local queue
		} else {
			jobQueue.add(job);
			sendJobEvent(job, ControlMessageType.JobArrival);
			writeToBinary(logfilename,job,true);
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
			sendJobEvent(waitingJob,ControlMessageType.JobStarted);
			writeToBinary(logfilename,waitingJob,true);
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
		sendJobEvent(job,ControlMessageType.JobCompleted);
		// job finished, remove it from our pool
		writeToBinary(logfilename,job,true);

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

	public String getLogFileName() {
		return logfilename;
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


		SynchronizedSocket syncSocket;
		syncSocket = new SynchronizedSocket(socketURL, socketPort);
		syncSocket.addMessageReceivedHandler(this);

		ControlMessage message = new ControlMessage(ControlMessageType.RMRequestsGSList, socketURL, socketPort);
		SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(message, new InetSocketAddress(gridSchedulerURL, gridSchedulerPort), this);
		syncClientSocket.sendMessage();

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

		//TODO Tirar o IF se for para ver os prints todos...
		if(controlMessage.getType() != ControlMessageType.RequestLoad) {
			System.out.println("[RM "+cluster.getID()+"] Message received: " + controlMessage.getType()+"\n");
		}


		// When Resource manager receives the list of all GS available from the GS that was given when this RM was initialized. The RM will try to join each of the GS
		if (controlMessage.getType() == ControlMessageType.ReplyGSList)
		{
			for (InetSocketAddress address:controlMessage.getGridSchedulersList()){
				if(gsList.replace(address, 0) == null){
					gsList.put(address, 0);
				} 
			}

			SynchronizedClientSocket syncClientSocket;
			System.out.println("GSList:" + gsList);
			for(InetSocketAddress address : gsList.keySet()) {
				ControlMessage msg = new ControlMessage(ControlMessageType.ResourceManagerJoin, this.socketURL, socketPort);
				msg.setClock(vClock.getClock());
				syncClientSocket = new SynchronizedClientSocket(msg, address, this);
				syncClientSocket.sendMessage();
			}

		}

		// ResourceManager receives ack that the GS added them to theirs GS list.
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoinAck)
		{
			//Handled on exception if it doesn't receives ack
		}


		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.RequestLoad)
		{
			ControlMessage replyMessage = new ControlMessage(ControlMessageType.ReplyLoad);
			replyMessage.setUrl(socketURL);
			replyMessage.setPort(socketPort);
			replyMessage.setLoad(jobQueue.size());
			//syncSocket.sendMessage(replyMessage, controlMessage.getInetAddress());	
			return replyMessage;
		}


		// RM receives add Job from a GS
		if (controlMessage.getType() == ControlMessageType.AddJob)
		{
			writeToBinary(logfilename,controlMessage.getJob(),true);

			jobQueue.add(controlMessage.getJob());

			vClock.updateClock(controlMessage.getClock(), identifier);

			//Now only sends message to the GS from where the message came from.
			ControlMessage msg;
			synchronized (this) {
				vClock.incrementClock(identifier);
				msg = new ControlMessage(ControlMessageType.JobArrival, controlMessage.getJob(), this.socketURL, this.socketPort);
				msg.setClock(vClock.getClock());
			}

			SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(msg, controlMessage.getInetAddress(), this);
			syncClientSocket.sendMessage();
			scheduleJobs();


			return null;

		}

		if (controlMessage.getType() == ControlMessageType.AddJobAck)
		{

			Timer t = jobTimers.remove(controlMessage.getJob().getId());
			if(t != null) t.cancel();

			return null;
		}

		/*
		if (controlMessage.getType() == ControlMessageType.GSLogJobArrival)
		{			
			//Logs

			ControlMessage replyMessage = new ControlMessage(ControlMessageType.GSLogJobArrivalAck);
			replyMessage.setUrl(socketURL);
			replyMessage.setPort(socketPort);
			//syncSocket.sendMessage(replyMessage, controlMessage.getInetAddress());	
			//return replyMessage;
			return null;

		}*/
		
		return null;
		
	}

	@Override
	public ControlMessage onExceptionThrown(Message message,
			InetSocketAddress destinationAddress) {
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage)message;
		gsList.replace(destinationAddress, gsList.get(destinationAddress)+1);
		if (gsList.get(destinationAddress) > 2) {
			//gsList.remove(destinationAddress);
			return null;
		}


		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {
			return controlMessage;
		}

		// if jobAdd fails it will add to the jobQueue again
		if (controlMessage.getType() == ControlMessageType.AddJob) {
			//this.jobQueue.add(controlMessage.getJob());
			addJob(controlMessage.getJob());//Adds job again to RM, if it has free space it can execute it, or it can send it again to a GS
			Timer t = jobTimers.remove(controlMessage.getJob().getId());
			if (t != null) t.cancel();
		}

		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.JobArrival ||
				controlMessage.getType() == ControlMessageType.JobStarted ||
				controlMessage.getType() == ControlMessageType.JobStarted) {

			SynchronizedClientSocket s = new SynchronizedClientSocket(controlMessage, getRandomGS() ,this);
			s.sendMessage();

			return controlMessage;
		}

		//Always tries to send to send the same message again.
		return null;

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

	public  void writeToBinary (String filename, Object obj, boolean append){
		File file = new File (filename);
		ObjectOutputStream out = null;

		try{
			if (!file.exists () || !append) out = new ObjectOutputStream (new FileOutputStream (filename));
			else out = new AppendableObjectOutputStream (new FileOutputStream (filename, append));
			out.writeObject(obj);
			out.flush ();
		}catch (Exception e){
			e.printStackTrace ();
		}finally{
			try{
				if (out != null) out.close ();
			}catch (Exception e){
				e.printStackTrace ();
			}
		}
	}

	public ArrayList<Job> readFromBinaryFile (String filename){
		File file = new File (filename);
		ArrayList<Job> recoveredLog = new ArrayList<Job>();
		if (file.exists ()){
			ObjectInputStream ois = null;
			try{
				ois = new ObjectInputStream (new FileInputStream (filename));
				while (true){

					Job j = (Job)ois.readObject ();
					recoveredLog.add(j);
				}
			}catch (EOFException e){

			}catch (Exception e){
				e.printStackTrace ();
			}finally{
				try{
					if (ois != null) ois.close();
				}catch (IOException e){
					e.printStackTrace ();
				}
			}
		}
		return recoveredLog;
	}

	private class AppendableObjectOutputStream extends ObjectOutputStream {
		public AppendableObjectOutputStream(OutputStream out) throws IOException {
			super(out);
		}

		@Override
		protected void writeStreamHeader() throws IOException {}
	}

	private InetSocketAddress getRandomGS() {
		return (InetSocketAddress)gsList.keySet().toArray()[(int)(Math.random() * ((gsList.size()-1) + 1))];
	}

	private synchronized void sendJobEvent(Job job, ControlMessageType messageType) {
		ControlMessage msg;
		synchronized(this) {
			msg = new ControlMessage(messageType, job, this.socketURL, this.socketPort);
			vClock.incrementClock(this.identifier);
			msg.setClock(vClock.getClock());
		}
		SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(msg, getRandomGS(), this);
		syncClientSocket.sendMessage();
	}

}
