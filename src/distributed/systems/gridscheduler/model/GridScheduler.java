package distributed.systems.gridscheduler.model;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import distributed.systems.core.IMessageReceivedHandler;
import distributed.systems.core.LogEntry;
import distributed.systems.core.LogManager;
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
	private String logfilename = "";

	// job queue
	private ConcurrentLinkedQueue<Job> jobQueue;

	private int identifier;
	
	// (max) number of entities (GS and RM/Clusters) present in the simulation
	private int nEntities;
	
	// number of jobs to be executed in the simulation
	private int nJobs;
	
	private VectorialClock vClock;

	// local hostname
	private  String hostname;
	// local port
	private  int port;
	
	// timeout to recieve an ACK (response) message
	private final int timeout = 1000;

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
	 * Constructs a new GridScheduler object at a given hostname and port number.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>url</CODE> cannot be null
	 * </DL>
	 * @param hostname the gridscheduler's hostname to register at
	 * @param port the gridscheduler's port to register at
	 */
	public GridScheduler(int id, int nEntities, int nJobs, String hostname, int port) {
		// preconditions
		assert(hostname != null) : "parameter 'url' cannot be null";
		assert(port > 0) : "parameter 'port'";
		assert(id >= 0);
		assert(nEntities > 0);

		//initialize the internal structure
		initilizeGridScheduler(id, nEntities, nJobs, hostname, port);

		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
	}

	public GridScheduler(int id, int nEntities, int nJobs, String hostname, int port, String otherGSHostname, int otherGSPort) {
		//preconditions
		assert(hostname != null) : "parameter 'hostname' cannot be null";
		assert(port > 0) : "parameter 'port' cannot be less than or equal to 0";
		assert(otherGSHostname != null) : "parameter 'otherGSHostname' cannot be null";
		assert(otherGSPort > 0) : "parameter 'otherGSPort' cannot be less than or equal to 0";
		assert(id >= 0) : "parameter 'id' cannot be less than 0";
		assert(nEntities > 0) : "parameter 'nEntities' should be greater than 0";

		//initialize internal structure
		initilizeGridScheduler(id, nEntities, nJobs, hostname, port);

		//in the case where another GS was provided, query that GS for the complete GS list
		ControlMessage cMessage =  new ControlMessage(ControlMessageType.GSRequestsGSList, hostname, port);

		//send the message querying the other GS
		SynchronizedClientSocket syncClientSocket;
		syncClientSocket = new SynchronizedClientSocket(cMessage, new InetSocketAddress(otherGSHostname, otherGSPort),this, timeout);
		syncClientSocket.sendMessage();

		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();

	}

	private void initilizeGridScheduler(int id, int nEntities, int nJobs, String hostname, int port){
		
		this.hostname = hostname;
		this.port = port;
		this.identifier = id;
		this.nJobs = nJobs;
		this.nEntities = nEntities;
		this.logfilename += "GS_" + id +".log";
		
		// TODO Como é que se vai fazer quanto aos Restart's?
		// Colocar uma flag para indicar se se trata de um restart ou não?
		//delete older log files
		File file = new File (logfilename);
		file.delete();

		gridSchedulersList = new HashSet<InetSocketAddress>();
		gridSchedulersList.add(new InetSocketAddress(hostname, port));

		this.resourceManagerLoad = new ConcurrentHashMap<InetSocketAddress, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.log = new ArrayList<LogEntry>();	
		vClock = new VectorialClock(nEntities);

		syncSocket = new SynchronizedSocket(hostname, port);
		syncSocket.addMessageReceivedHandler(this);

		// Thread that checks if the simulation is over.
		new Thread(new Runnable() {
			public void run() {
				//TODO Check if the simulation is over using the number of jobs variable.
			}
		}).start();

	}

	/**
	 * The gridscheduler's name also doubles as its URL in the local messaging system.
	 * It is passed to the constructor and cannot be changed afterwards.
	 * @return the name of the gridscheduler
	 */
	public String getHostname() {
		return hostname;
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
		VectorialClock tempVC;
		ControlMessage msg = null;

		//TODO Sincronizacao do log... Ver as mensagens que tem de ser logadas.
		// Chamar um metodo que faca isto nos locais adequados.

		if(controlMessage.getType() != ControlMessageType.ReplyLoad) {
			System.out.println("[GS "+hostname+":"+port+"] Message received: " + controlMessage.getType()+"\n");
		}

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
			msg = new ControlMessage(ControlMessageType.ReplyGSList, hostname, port);
			msg.setGridSchedulersList(gridSchedulersList);
			//syncSocket.sendMessage(msg, controlMessage.getInetAddress());
			return msg;
		}		

		if (controlMessage.getType() == ControlMessageType.GSRequestsGSList) {

			Set<InetSocketAddress> gridSchedulersListTemp = gridSchedulersList;
			gridSchedulersList.add(controlMessage.getInetAddress());
			msg = new ControlMessage(ControlMessageType.ReplyGSList, hostname, port);
			msg.setGridSchedulersList(gridSchedulersList);	

			for(InetSocketAddress address : gridSchedulersListTemp) {
				if (address.getPort() == this.getPort()) continue;
				syncClientSocket = new SynchronizedClientSocket(msg, address, this, timeout);
				syncClientSocket.sendMessageWithoutResponse();
			}			
			return msg;
		}

		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob) {
			//TODO aqui precisa de log?
			vClock.updateClock(controlMessage.getClock(), identifier);
			jobQueue.add(controlMessage.getJob());
			//Syncing
		}

		if (controlMessage.getType() == ControlMessageType.JobArrival) {			

			vClock.updateClock(controlMessage.getClock(), identifier);
			tempVC = vClock;

			//TODO Logar accao...
			LogManager.writeToBinary(logfilename,controlMessage,true);

			synchronizeWIthAllGS(new ControlMessage(ControlMessageType.GSLogJobArrival, hostname, port));

			if(!controlMessage.getJob().getOriginalRM().equals(controlMessage.getInetAddress())) {
				// Prepare a different message but mantain the clock.
				synchronized(this) {
					msg = new ControlMessage(ControlMessageType.AddJobAck, controlMessage.getJob(), hostname, port);
					msg.setClock(tempVC.getClock());
				}
				syncClientSocket = new SynchronizedClientSocket(msg, controlMessage.getJob().getOriginalRM(), this, timeout);
				syncClientSocket.sendMessageWithoutResponse();			
			}

			msg = new ControlMessage(ControlMessageType.JobArrivalAck, hostname, port);
			msg.setClock(tempVC.getClock());
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogJobArrival) {
			vClock.updateClock(controlMessage.getClock(), identifier);
			
			//TODO Fazer log...
			LogManager.writeToBinary(logfilename,controlMessage,true);

			return prepareMessageToSend(new ControlMessage(ControlMessageType.GSLogJobArrivalAck, hostname, port));
		}

		if (controlMessage.getType() == ControlMessageType.JobStarted) {
			vClock.updateClock(controlMessage.getClock(), identifier);
			tempVC = vClock;
			
			//TODO Logar accao...
			LogManager.writeToBinary(logfilename,controlMessage,true);

			synchronizeWIthAllGS(new ControlMessage(ControlMessageType.GSLogJobStarted, hostname, port));
			msg = new ControlMessage(ControlMessageType.JobStartedAck, hostname, port);
			msg.setClock(tempVC.getClock());
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogJobStarted) {				
			vClock.updateClock(controlMessage.getClock(), identifier);

			//TODO Fazer log...
			LogManager.writeToBinary(logfilename,controlMessage,true);

			return prepareMessageToSend(new ControlMessage(ControlMessageType.GSLogJobStartedAck, hostname, port));
		}

		if (controlMessage.getType() == ControlMessageType.JobCompleted) {
			vClock.updateClock(controlMessage.getClock(), identifier);
			tempVC = vClock;
			
			//TODO Logar accao...
			LogManager.writeToBinary(logfilename,controlMessage,true);

			synchronizeWIthAllGS(new ControlMessage(ControlMessageType.GSLogJobCompleted, hostname, port));
			msg = new ControlMessage(ControlMessageType.JobCompletedAck, hostname, port);
			msg.setClock(tempVC.getClock());
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogJobCompleted) {				
			
			vClock.updateClock(controlMessage.getClock(), identifier);
			
			//TODO Fazer log...
			LogManager.writeToBinary(logfilename,controlMessage,true);

			return prepareMessageToSend(new ControlMessage(ControlMessageType.GSLogJobCompletedAck, hostname, port));
		}

		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {
			resourceManagerLoad.put(controlMessage.getInetAddress(), Integer.MAX_VALUE);
			vClock.updateClock(controlMessage.getClock(), identifier);
			return prepareMessageToSend(new ControlMessage(ControlMessageType.ResourceManagerJoinAck, hostname, port));
		}

		//Receives LogEntry from another GridScheduler
		if (controlMessage.getType() == ControlMessageType.GSSendLogEntry)
		{
			this.logEntry = controlMessage.getLogEntry();
			log.add(this.logEntry);
		}

		return null;

	}

	/**
	 * onExceptionThrown return null if there is no message to be resent, or return the message to be sent
	 * @param message Original message that was sent before the exception.
	 * @param address Address from the machine where the orignal message was being sent.
	 */
	@Override
	public synchronized ControlMessage onExceptionThrown(Message message, InetSocketAddress address) {
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage)message;

		//TODO O RM vai ser removido para sempre... Solução?
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
				cMessage.setHostname(this.getHostname());
				cMessage.setPort(this.getPort());

				//syncSocket.sendMessage(cMessage, inetAdd);
				SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(cMessage, inetAdd, this, timeout);
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
						cMessage = new ControlMessage(ControlMessageType.AddJob, job,this.getHostname(), this.getPort());					
						cMessage.setClock(vClock.getClock());
					}

					SynchronizedClientSocket syncClientSocket = new SynchronizedClientSocket(cMessage, leastLoadedRM, this, timeout);
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
	private synchronized void logMessage(ControlMessage message) {

		//TODO Sincronizao do log... Ver as mensagens que tim de ser logadas.
		// Chamar um metodo que faz isto nos locais adequados.

		if(message.getType() != ControlMessageType.ReplyLoad && message.getType() != ControlMessageType.GSSendLogEntry ){
			this.logEntry = new LogEntry(message);
			log.add(this.logEntry);

			ControlMessage msg = new ControlMessage(ControlMessageType.GSSendLogEntry, this.logEntry, hostname, port);
			SynchronizedClientSocket syncClientSocket;
			for(InetSocketAddress address : gridSchedulersList) {
				if (address.getHostName() == this.getHostname() && address.getPort() == this.getPort()) continue; //Doe
				System.out.println("Sending logEntry from: "+ this.hostname +":"+ this.port +"to:" + address.toString());
				syncClientSocket = new SynchronizedClientSocket(msg, address, this, timeout);
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

	private synchronized ControlMessage prepareMessageToSend(ControlMessage msg) {
		vClock.incrementClock(identifier);
		msg.setClock(vClock.getClock());
		return msg;
	}

	private void synchronizeWIthAllGS(ControlMessage messageToSend) {

		SyncLog syncLog = new SyncLog();
		SynchronizedClientSocket syncClientSocket;

		ControlMessage msg = prepareMessageToSend(messageToSend);

		for(InetSocketAddress address : gridSchedulersList) {
			if (address.getHostString() == this.getHostname() && address.getPort() == this.getPort()) continue;
			syncClientSocket = new SynchronizedClientSocket(msg, address, this, timeout);
			syncClientSocket.sendLogMessage(syncLog);
		}

		//Assume that it always gets a response from at least one of the GS
		if(gridSchedulersList.size() > 1) 
			syncLog.check();
		
	}
	
	public static void main(String[] args) {

		String usage = "Usage: GridScheduler <id> <nEntities> <nJobs> <hostname> <port> [<otherGSHostname> <otherGSPort>]";

		if(args.length != 5 && args.length != 7) {
			System.out.println(usage);
			System.exit(1);
		}

		try {
			if(args.length == 5) {
				new GridScheduler(
						Integer.parseInt(args[0]), 
						Integer.parseInt(args[1]),
						Integer.parseInt(args[2]),
						args[3], 
						Integer.parseInt(args[4]));
			}
			else if (args.length == 7) {
				new GridScheduler(
						Integer.parseInt(args[0]), 
						Integer.parseInt(args[1]),
						Integer.parseInt(args[2]),
						args[3], 
						Integer.parseInt(args[4]),
						args[5], 
						Integer.parseInt(args[6]));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(usage);
			System.exit(1);
		}
	}


}
