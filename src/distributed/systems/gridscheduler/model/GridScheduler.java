package distributed.systems.gridscheduler.model;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;
import distributed.systems.core.Socket;
import distributed.systems.core.SynchronizedSocket;
import distributed.systems.example.LocalSocket;

/**
 * 
 * The GridScheduler class represents a single-server implementation of the grid scheduler in the
 * virtual grid system.
 * 
 * @author Niels Brouwers
 *
 */
public class GridScheduler implements IMessageReceivedHandler, Runnable {
	
	// job queue
	private ConcurrentLinkedQueue<Job> jobQueue;
	
	// local url
	private final String url;

	// communications socket
	private Socket socket;
	
	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<String, Integer> resourceManagerLoad;

	// polling frequency, 1hz
	private long pollSleep = 1000;
	
	// polling thread
	private Thread pollingThread;
	private boolean running;
	
	/**
	 * Constructs a new GridScheduler object at a given url.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>url</CODE> cannot be null
	 * </DL>
	 * @param url the gridscheduler's url to register at
	 */
	public GridScheduler(String url) {
		// preconditions
		assert(url != null) : "parameter 'url' cannot be null";
		
		// init members
		this.url = url;
		this.resourceManagerLoad = new ConcurrentHashMap<String, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		
		// create a messaging socket
		LocalSocket lSocket = new LocalSocket();
		socket = new SynchronizedSocket(lSocket);
		socket.addMessageReceivedHandler(this);
		
		// register the socket under the name of the gridscheduler.
		// In this way, messages can be sent between components by name.
		socket.register(url);

		// start the polling thread
		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
	}
	
	/**
	 * The gridscheduler's name also doubles as its URL in the local messaging system.
	 * It is passed to the constructor and cannot be changed afterwards.
	 * @return the name of the gridscheduler
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * Gets the number of jobs that are waiting for completion.
	 * @return
	 */
	public int getWaitingJobs() {
		int ret = 0;
		ret = jobQueue.size();
		return ret;
	}

	/**
	 * Receives a message from another component.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>message</CODE> should be of type ControlMessage 
	 * <DD>parameter <CODE>message</CODE> should not be null
	 * </DL> 
	 * @param message a message
	 */
	public void onMessageReceived(Message message) {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";
		
		ControlMessage controlMessage = (ControlMessage)message;
		
		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin)
			resourceManagerLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
		
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob)
			jobQueue.add(controlMessage.getJob());
			
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.ReplyLoad)
			resourceManagerLoad.put(controlMessage.getUrl(),controlMessage.getLoad());
			
		
	}

	// finds the least loaded resource manager and returns its url
	private String getLeastLoadedRM() {
		String ret = null; 
		int minLoad = Integer.MAX_VALUE;
		
		// loop over all resource managers, and pick the one with the lowest load
		for (String key : resourceManagerLoad.keySet())
		{
			if (resourceManagerLoad.get(key) <= minLoad)
			{
				ret = key;
				minLoad = resourceManagerLoad.get(key);
			}
		}
		
		return ret;		
	}

	/**
	 * Polling thread runner. This thread polls each resource manager in turn to get its load,
	 * then offloads any job in the waiting queue to that resource manager
	 */
	public void run() {
		while (running) {
			// send a message to each resource manager, requesting its load
			for (String rmUrl : resourceManagerLoad.keySet())
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
				cMessage.setUrl(this.getUrl());
				socket.sendMessage(cMessage, "localsocket://" + rmUrl);
			}
			
			// schedule waiting messages to the different clusters
			for (Job job : jobQueue)
			{
				String leastLoadedRM =  getLeastLoadedRM();
				
				if (leastLoadedRM!=null) {
				
					ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
					cMessage.setJob(job);
					socket.sendMessage(cMessage, "localsocket://" + leastLoadedRM);
					
					jobQueue.remove(job);
					
					// increase the estimated load of that RM by 1 (because we just added a job)
					int load = resourceManagerLoad.get(leastLoadedRM);
					resourceManagerLoad.put(leastLoadedRM, load+1);
					
				}
				
			}
			
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
	
}
