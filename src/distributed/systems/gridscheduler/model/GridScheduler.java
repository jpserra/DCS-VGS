package distributed.systems.gridscheduler.model;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.Message;

import java.awt.TrayIcon.MessageType;
import java.net.InetSocketAddress;
import distributed.systems.core.SynchronizedSocket;
//import distributed.systems.example.LocalSocket;

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
	// local port
	private final int port;
	
	private Set<InetSocketAddress> gridSchedulersList;
	
	// communications socket
	private SynchronizedSocket syncSocket;
	
	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<InetSocketAddress, Integer> resourceManagerLoad;

	// polling frequency, 1hz
	private long pollSleep = 100;
	
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
	public GridScheduler(String url, int port) {
		// preconditions
		assert(url != null) : "parameter 'url' cannot be null";
		assert(port > 0) : "parameter 'port'";

		// init members
		this.url = url;
		this.port = port;
		
		gridSchedulersList = new HashSet<InetSocketAddress>();
		gridSchedulersList.add(new InetSocketAddress(url, port));
		
		this.resourceManagerLoad = new ConcurrentHashMap<InetSocketAddress, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
				
		syncSocket = new SynchronizedSocket(url, port);
		syncSocket.addMessageReceivedHandler(this);

		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
	}
	
	public GridScheduler(String url, int port, String otherGSUrl, int otherGSPort) {
		// preconditions
		assert(url != null) : "parameter 'url' cannot be null";
		assert(port > 0) : "parameter 'port'";
		assert(otherGSUrl != null) : "parameter 'url' cannot be null";
		assert(otherGSPort > 0) : "parameter 'port'";

		// init members
		this.url = url;
		this.port = port;
		
		gridSchedulersList = new HashSet<InetSocketAddress>();
		gridSchedulersList.add(new InetSocketAddress(url, port));
		
		this.resourceManagerLoad = new ConcurrentHashMap<InetSocketAddress, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
				
		syncSocket = new SynchronizedSocket(url, port);
		syncSocket.addMessageReceivedHandler(this);
		
		ControlMessage cMessage =  new ControlMessage(ControlMessageType.GSRequestsGSList);
		cMessage.setUrl(url);
		cMessage.setPort(port);	
		syncSocket.sendMessage(cMessage, new InetSocketAddress(otherGSUrl, otherGSPort));
		
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
	
	public int getPort() {
		return port;
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
		System.out.println("GS: Message received:" + controlMessage.getType());

		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {
			resourceManagerLoad.put(controlMessage.getInetAddress(), Integer.MAX_VALUE);
		}
		
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob)
			jobQueue.add(controlMessage.getJob());
			
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.ReplyLoad)
			resourceManagerLoad.put(controlMessage.getInetAddress(),controlMessage.getLoad());
			
		if (controlMessage.getType() == ControlMessageType.RMRequestsGSList) {
			ControlMessage msg = new ControlMessage(ControlMessageType.ReplyGSList);
			msg.setGridSchedulersList(gridSchedulersList);
			syncSocket.sendMessage(msg, controlMessage.getInetAddress());
		}
		
		if (controlMessage.getType() == ControlMessageType.GSRequestsGSList) {
			gridSchedulersList.add(controlMessage.getInetAddress());
			for(InetSocketAddress address : gridSchedulersList) {
				ControlMessage msg = new ControlMessage(ControlMessageType.ReplyGSList);
				msg.setGridSchedulersList(gridSchedulersList);
				syncSocket.sendMessage(msg, address);
			}
		}

		
		if (controlMessage.getType() == ControlMessageType.ReplyGSList)
		{
			for(InetSocketAddress address : controlMessage.getGridSchedulersList()) {
				gridSchedulersList.add(address);
			}
			System.out.println(gridSchedulersList);
		}
		
	}

	// finds the least loaded resource manager and returns its url
	private InetSocketAddress getLeastLoadedRM() {
		InetSocketAddress ret = null; 
		int minLoad = Integer.MAX_VALUE;
		
		// loop over all resource managers, and pick the one with the lowest load
		for (InetSocketAddress key : resourceManagerLoad.keySet())
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
			for (InetSocketAddress inetAdd : resourceManagerLoad.keySet())
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
				cMessage.setUrl(this.getUrl());
				cMessage.setPort(this.getPort());

				syncSocket.sendMessage(cMessage, inetAdd);
			}
			
			// schedule waiting messages to the different clusters
			for (Job job : jobQueue)
			{
				InetSocketAddress leastLoadedRM =  getLeastLoadedRM();
				
				if (leastLoadedRM!=null) {
				
					ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
					cMessage.setJob(job);
					syncSocket.sendMessage(cMessage, leastLoadedRM);
					
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
