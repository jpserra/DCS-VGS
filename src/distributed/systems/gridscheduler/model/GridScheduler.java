package distributed.systems.gridscheduler.model;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.LogEntry;
import distributed.systems.core.Message;
import distributed.systems.core.SynchronizedClientSocket;
import distributed.systems.core.SynchronizedSocket;
import distributed.systems.core.VectorialClock;

/**
 * 
 * The GridScheduler class represents a single-server implementation of the grid scheduler in the
 * virtual grid system.
 * 
 * @author Niels Brouwers
 *
 */
public class GridScheduler implements IMessageReceivedHandler, Runnable {
	
	//General Log containing every happening
	private ArrayList<LogEntry> log;
	private LogEntry logEntry;
	
	// job queue
	private ConcurrentLinkedQueue<Job> jobQueue;
	
	private int identifier;
	private int nEntities;
	private VectorialClock vClock;

	// local url
	private  String url;
	// local port
	private  int port;
	
	private Set<InetSocketAddress> gridSchedulersList;
		
	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<InetSocketAddress, Integer> resourceManagerLoad;
	
	// polling frequency, 1hz
	private long pollSleep = 100;
	
	// polling thread
	private Thread pollingThread;
	private boolean running;
	
	SynchronizedSocket syncSocket;
	
	/**
	 * Constructs a new GridScheduler object at a given url.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>url</CODE> cannot be null
	 * </DL>
	 * @param url the gridscheduler's url to register at
	 */
	public GridScheduler(int id, int nEntities, String url, int port) {
		// preconditions
		assert(url != null) : "parameter 'url' cannot be null";
		assert(port > 0) : "parameter 'port'";
		
		initilizeGridScheduler(id, nEntities, url, port);

		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
	}
	
	public GridScheduler(int id, int nEntities, String url, int port, String otherGSUrl, int otherGSPort) {
		// preconditions
		assert(url != null) : "parameter 'url' cannot be null";
		assert(port > 0) : "parameter 'port'";
		assert(otherGSUrl != null) : "parameter 'url' cannot be null";
		assert(otherGSPort > 0) : "parameter 'port'";

		
		initilizeGridScheduler(id, nEntities, url, port);

		
		ControlMessage cMessage =  new ControlMessage(ControlMessageType.GSRequestsGSList, url, port);
		
		SynchronizedClientSocket syncClientSocket;
		//Usar um socket diferente para fazer o envio das mensagens.
		syncClientSocket = new SynchronizedClientSocket(cMessage, new InetSocketAddress(otherGSUrl, otherGSPort),this);
		syncClientSocket.sendMessage();
		
		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
		
	}
	
	private void initilizeGridScheduler(int id, int nEntities,String url, int port){
		// init members
		this.url = url;
		this.port = port;
		identifier = id;
		this.nEntities = nEntities;

		gridSchedulersList = new HashSet<InetSocketAddress>();
		gridSchedulersList.add(new InetSocketAddress(url, port));
		
		this.resourceManagerLoad = new ConcurrentHashMap<InetSocketAddress, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.log = new ArrayList<LogEntry>();	
		vClock = new VectorialClock(nEntities);

		syncSocket = new SynchronizedSocket(url, port);
		syncSocket.addMessageReceivedHandler(this);
		
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
	public ControlMessage onMessageReceived(Message message) {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";
		
		SynchronizedClientSocket syncClientSocket;
		ControlMessage controlMessage = (ControlMessage)message;
		
		//TODO Sincronizacao do log... Ver as mensagens que tem de ser logadas.
		// Chamar um metodo que faca isto nos locais adequados.
		//this.logMessage(controlMessage);
		
		if(controlMessage.getType() != ControlMessageType.ReplyLoad) {
			System.out.println("[GS "+url+":"+port+"] Message received: " + controlMessage.getType()+"\n");
		}

		
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.ReplyLoad)
			resourceManagerLoad.put(controlMessage.getInetAddress(),controlMessage.getLoad());
			
		
		if (controlMessage.getType() == ControlMessageType.ReplyGSList)
		{
			synchronized (gridSchedulersList) {
		
			for(InetSocketAddress address : controlMessage.getGridSchedulersList()) {
				gridSchedulersList.add(address);
			}
			}
			System.out.println(gridSchedulersList);
		}
		
		if (controlMessage.getType() == ControlMessageType.RMRequestsGSList) {
			ControlMessage msg = new ControlMessage(ControlMessageType.ReplyGSList, url, port);
			msg.setGridSchedulersList(gridSchedulersList);
			//syncSocket.sendMessage(msg, controlMessage.getInetAddress());
			return msg;
		}		
		
		if (controlMessage.getType() == ControlMessageType.GSRequestsGSList) {
			
			Set<InetSocketAddress> gridSchedulersListTemp = gridSchedulersList;
			gridSchedulersList.add(controlMessage.getInetAddress());
			ControlMessage msg = new ControlMessage(ControlMessageType.ReplyGSList, url, port);
			msg.setGridSchedulersList(gridSchedulersList);	
			
			for(InetSocketAddress address : gridSchedulersListTemp) {
				if (address.getPort() == this.getPort()) continue;
				syncClientSocket = new SynchronizedClientSocket(msg, address, this);
				syncClientSocket.sendMessageWithoutResponse();
			}			
			return msg;
		}
		
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob) {			
			jobQueue.add(controlMessage.getJob());
			
			
			
			//Syncing
			
			//ControlMessage msg = new ControlMessage(ControlMessageType.AddJobAck, url, port);
			//return null;
		}
		
		
		
		if (controlMessage.getType() == ControlMessageType.JobArrival) {			
			SyncLog syncLog = new SyncLog();

			for(InetSocketAddress address : gridSchedulersList) {
				if (address.getHostString() == this.getUrl() && address.getPort() == this.getPort()) continue;
				ControlMessage msg = new ControlMessage(ControlMessageType.GSLogJobArrival, url, port);
				syncClientSocket = new SynchronizedClientSocket(msg, address, this);
				syncClientSocket.sendLogMessage(syncLog);
				//System.out.println("Envia pedido de liberdade...");

			}
			if(gridSchedulersList.size() > 1)syncLog.check();//Assume that it always gets a response from at least one of the GS	
			
			if(!controlMessage.getJob().getOriginalRM().equals(controlMessage.getInetAddress())) {
				ControlMessage msg = new ControlMessage(ControlMessageType.AddJobAck, url, port);
				msg.setJob(controlMessage.getJob());
				syncClientSocket = new SynchronizedClientSocket(msg, controlMessage.getJob().getOriginalRM(), this);
				syncClientSocket.sendMessageWithoutResponse();			
			}
			
			return new ControlMessage(ControlMessageType.JobArrivalAck, url, port);
		}
		
		if (controlMessage.getType() == ControlMessageType.GSLogJobArrival) {			
			
			//Logs message
 
			return new ControlMessage(ControlMessageType.GSLogJobArrivalAck, url, port);
		}
		
		if (controlMessage.getType() == ControlMessageType.JobStarted) {			
			SyncLog syncLog = new SyncLog();

			for(InetSocketAddress address : gridSchedulersList) {
				if (address.getHostString() == this.getUrl() && address.getPort() == this.getPort()) continue;
				ControlMessage msg = new ControlMessage(ControlMessageType.GSLogJobStarted, url, port);
				syncClientSocket = new SynchronizedClientSocket(msg, address, this);
				syncClientSocket.sendLogMessage(syncLog);
			}
			syncLog.check();		
						
			return new ControlMessage(ControlMessageType.JobStartedAck, url, port);
		}
		
		if (controlMessage.getType() == ControlMessageType.GSLogJobStarted) {				
			//Logs message
			return new ControlMessage(ControlMessageType.GSLogJobStartedAck, url, port);
		}
		
		if (controlMessage.getType() == ControlMessageType.JobCompleted) {			
			SyncLog syncLog = new SyncLog();

			for(InetSocketAddress address : gridSchedulersList) {
				if (address.getHostString() == this.getUrl() && address.getPort() == this.getPort()) continue;
				ControlMessage msg = new ControlMessage(ControlMessageType.GSLogJobCompleted, url, port);
				syncClientSocket = new SynchronizedClientSocket(msg, address, this);
				syncClientSocket.sendLogMessage(syncLog);
			}
			syncLog.check();		
						
			return new ControlMessage(ControlMessageType.JobCompletedAck, url, port);
		}
		
		if (controlMessage.getType() == ControlMessageType.GSLogJobCompleted) {				
			//Logs message
			return new ControlMessage(ControlMessageType.GSLogJobCompletedAck, url, port);
		}
		
		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {
					resourceManagerLoad.put(controlMessage.getInetAddress(), Integer.MAX_VALUE);
			ControlMessage msg = new ControlMessage(ControlMessageType.ResourceManagerJoinAck, url, port);
			return msg;

		}
		
		
		//Receives LogEntry from another GridScheduler
		if (controlMessage.getType() == ControlMessageType.GSSendLogEntry)
		{
			this.logEntry = controlMessage.getLogEntry();
			log.add(this.logEntry);
			
		}
				
		return null;
	}
		
	
	/*
	 * 
	 * onExceptionThrown return null if there is no message to be resent, or return the message to be sent
	 * 
	 * */
	@Override
	public synchronized ControlMessage onExceptionThrown(Message message, InetSocketAddress address) {
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";
		
		ControlMessage controlMessage = (ControlMessage)message;
		
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob) {	
			resourceManagerLoad.remove(address);
			jobQueue.add(controlMessage.getJob());
		}
		
		return null;
		
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

				//syncSocket.sendMessage(cMessage, inetAdd);
				SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(cMessage, inetAdd, this);
				syncClientSocket.sendMessage();

			}
			
			// schedule waiting messages to the different clusters
			for (Job job : jobQueue)
			{
				InetSocketAddress leastLoadedRM =  getLeastLoadedRM();
				
				if (leastLoadedRM!=null) {
				
					ControlMessage cMessage ;
					synchronized (this) {
						vClock.incrementClock(identifier);
						cMessage = new ControlMessage(ControlMessageType.AddJob, job,this.getUrl(), this.getPort());					
						cMessage.setClock(vClock.getClock());
					}
					
					SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(cMessage, leastLoadedRM, this);
					jobQueue.remove(job);
					syncClientSocket.sendMessageWithoutResponse();
					
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

	//Method called when a message arrives
	private synchronized void  logMessage(ControlMessage message) {
			
		//TODO Sincronizao do log... Ver as mensagens que tim de ser logadas.
				// Chamar um metodo que fala isto nos locais adequados.
			
		if(message.getType() != ControlMessageType.ReplyLoad && message.getType() != ControlMessageType.GSSendLogEntry ){
			this.logEntry = new LogEntry(message);
			log.add(this.logEntry);
		
			ControlMessage msg = new ControlMessage(ControlMessageType.GSSendLogEntry, this.logEntry, url, port);
			SynchronizedClientSocket syncClientSocket;
			for(InetSocketAddress address : gridSchedulersList) {
				if (address.getHostName() == this.getUrl() && address.getPort() == this.getPort()) continue; //Doe
				System.out.println("Sending logEntry from: "+ this.url +":"+ this.port +"to:" + address.toString());
				syncClientSocket = new SynchronizedClientSocket(msg, address, this);
				syncClientSocket.sendMessage();
			}		
		}	
	}
	
	 /*
     * Returns the entire history of messages saves on the Grid Scheduler Log
     */
    public ArrayList<LogEntry> getFullLog(){
            return log;
    }
		
}
