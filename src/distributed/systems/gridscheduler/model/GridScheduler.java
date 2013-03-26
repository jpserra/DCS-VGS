package distributed.systems.gridscheduler.model;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
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
	//private ArrayList<LogEntry> log;
	private LogEntry logEntry;
	private String logfilename = "";
	private LogManager logger;

	// job queue
	private ConcurrentLinkedQueue<Job> jobQueue;

	private int identifier;

	// (max) number of entities (GS and RM/Clusters) present in the simulation
	private int nEntities;

	// number of jobs to be executed in the simulation
	private int nJobs;

	// number of jobs finished
	private int jobsFinished = 0;

	private VectorialClock vClock;

	// local hostname
	private  String hostname;
	// local port
	private  int port;

	// timeout to recieve an ACK (response) message
	private final int timeout = 1000;

	private ConcurrentHashMap<InetSocketAddress, Integer> gridSchedulersList;

	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<InetSocketAddress, Integer> resourceManagerLoad;
	
	private Set<Long> finishedJobs;

	private long checkThreadPollSleep = 1000;

	// polling frequency, 1hz
	private long pollSleep = 100;
	
	private GridScheduler handler;

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

	private void launchCheckThread() {
		// Thread that checks if the simulation is over.
		new Thread(new Runnable() {
			public void run() {
				while(jobsFinished < nJobs) {
					try {
						Thread.sleep(checkThreadPollSleep);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				//TODO Enviar mensagem aos RM's conectados a indicar que a simulacao acabou.
				System.out.println("GS <"+hostname+":"+port+">: A SIMULACAO ACABOU!");
				ControlMessage message;
				SynchronizedClientSocket syncClientSocket;
				for(InetSocketAddress address : resourceManagerLoad.keySet()) {
					message = new ControlMessage(ControlMessageType.SimulationOver,hostname, port);
					syncClientSocket = new SynchronizedClientSocket(message, address, handler, timeout);
					syncClientSocket.sendMessage();
				}
			}
		}).start();
	}

	private void initilizeGridScheduler(int id, int nEntities, int nJobs, String hostname, int port){

		this.hostname = hostname;
		this.port = port;
		this.identifier = id;
		this.nJobs = nJobs;
		this.nEntities = nEntities;
		this.logfilename += "GS_" + id +".log";
		this.handler = this;

		this.logger = new LogManager(logfilename);
		// TODO Como  que se vai fazer quanto aos Restart's?
		// Colocar uma flag para indicar se se trata de um restart ou no?
		// Tratar as situaes de forma diferente depois...
		//delete older log files
		File file = new File (logfilename);
		file.delete();

		gridSchedulersList = new ConcurrentHashMap<InetSocketAddress,Integer>();
		gridSchedulersList.put(new InetSocketAddress(hostname, port),0);
		finishedJobs = new HashSet<Long>();

		this.resourceManagerLoad = new ConcurrentHashMap<InetSocketAddress, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		//this.log = new ArrayList<LogEntry>();	
		vClock = new VectorialClock(nEntities);

		syncSocket = new SynchronizedSocket(hostname, port);
		syncSocket.addMessageReceivedHandler(this);

		launchCheckThread();

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

	public String getLogFileName() {
		return logfilename;
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
		VectorialClock tempVC = new VectorialClock(nEntities);
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
					gridSchedulersList.put(address,0);
				}
			}
			System.out.println(gridSchedulersList);
		}

		if (controlMessage.getType() == ControlMessageType.RMRequestsGSList) {
			msg = new ControlMessage(ControlMessageType.ReplyGSList, hostname, port);
			msg.setGridSchedulersList(gridSchedulersList.keySet());
			return msg;
		}		

		if (controlMessage.getType() == ControlMessageType.GSRequestsGSList) {
			gridSchedulersList.put(controlMessage.getInetAddress(),0);
			msg = new ControlMessage(ControlMessageType.ReplyGSList, hostname, port);
			msg.setGridSchedulersList(gridSchedulersList.keySet());	
			for(InetSocketAddress address : gridSchedulersList.keySet()) {
				if (address.getPort() == this.getPort() && address.getHostName() == this.getHostname()) continue;
				if (address.equals(controlMessage.getInetAddress())) continue;
				syncClientSocket = new SynchronizedClientSocket(msg, address, this, timeout);
				syncClientSocket.sendMessageWithoutResponse();
			}			
			return msg;
		}

		// resource manager wants to offload a job to us
		// there is no need to log because the RM is waiting for an ACK.
		// The ACK message will only be sent when the Job is delegated to another RM.
		if (controlMessage.getType() == ControlMessageType.AddJob) {
			// Tries to add the RM to the list if this was not there already.
			if(!resourceManagerLoad.containsKey(controlMessage.getInetAddress())) {
				resourceManagerLoad.put(controlMessage.getInetAddress(), Integer.MAX_VALUE);
			}
			vClock.updateClock(controlMessage.getClock());
			jobQueue.add(controlMessage.getJob());
		}

		if (controlMessage.getType() == ControlMessageType.JobArrival) {			
			// Backup the original clock that was recieved when the event arrived
			tempVC.setClock(vClock.updateClock(controlMessage.getClock()));
			
			logger.writeToBinary(new LogEntry(controlMessage),true);

			synchronizeWithAllGS(new ControlMessage(ControlMessageType.GSLogJobArrival, controlMessage.getJob(), hostname, port));

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
			gridSchedulersList.put(controlMessage.getInetAddress(), 0);
			vClock.updateClock(controlMessage.getClock());
			logger.writeToBinary(new LogEntry(controlMessage),true);
			return prepareMessageToSend(new ControlMessage(ControlMessageType.GSLogJobArrivalAck, hostname, port));
		}

		if (controlMessage.getType() == ControlMessageType.JobStarted) {
			// Update the clock and store it in a temporary one.
			tempVC.setClock(vClock.updateClock(controlMessage.getClock()));
			logger.writeToBinary(new LogEntry(controlMessage),true);
			synchronizeWithAllGS(new ControlMessage(ControlMessageType.GSLogJobStarted, controlMessage.getJob(), hostname, port));
			msg = new ControlMessage(ControlMessageType.JobStartedAck, hostname, port);
			msg.setClock(tempVC.getClock());
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogJobStarted) {
			gridSchedulersList.put(controlMessage.getInetAddress(), 0);
			vClock.updateClock(controlMessage.getClock());
			logger.writeToBinary(new LogEntry(controlMessage),true);
			return prepareMessageToSend(new ControlMessage(ControlMessageType.GSLogJobStartedAck, hostname, port));
		}

		if (controlMessage.getType() == ControlMessageType.JobCompleted) {
			// Update the clock and store it in a temporary one.
			tempVC.setClock(vClock.updateClock(controlMessage.getClock()));
			logger.writeToBinary(new LogEntry(controlMessage),true);
			if(finishedJobs.add(controlMessage.getJob().getId())) {
				jobsFinished++;
			}
			synchronizeWithAllGS(new ControlMessage(ControlMessageType.GSLogJobCompleted, controlMessage.getJob(), hostname, port));
			msg = new ControlMessage(ControlMessageType.JobCompletedAck, hostname, port);
			msg.setClock(tempVC.getClock());
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogJobCompleted) {
			gridSchedulersList.put(controlMessage.getInetAddress(), 0);
			vClock.updateClock(controlMessage.getClock());
			logger.writeToBinary(new LogEntry(controlMessage),true);
			jobsFinished++;
			return prepareMessageToSend(new ControlMessage(ControlMessageType.GSLogJobCompletedAck, hostname, port));
		}

		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {
			resourceManagerLoad.put(controlMessage.getInetAddress(), Integer.MAX_VALUE);
			vClock.updateClock(controlMessage.getClock());
			return prepareMessageToSend(new ControlMessage(ControlMessageType.ResourceManagerJoinAck, hostname, port));
		}



		return null;

	}

	@Override
	public ControlMessage onConnectExceptionThrown(Message message,
			InetSocketAddress destinationAddress, boolean requiresRepsonse) {
		ControlMessage controlMessage = (ControlMessage)message;
		if (controlMessage.getType() == ControlMessageType.GSLogJobArrival ||
				controlMessage.getType() == ControlMessageType.GSLogJobCompleted ||
				controlMessage.getType() == ControlMessageType.GSLogJobStarted){

			if(checkGSFailures(destinationAddress)) {
				SynchronizedClientSocket client = new SynchronizedClientSocket(controlMessage, destinationAddress, this, timeout);
				client.sendMessage();
			}
		}

		if (controlMessage.getType() == ControlMessageType.AddJob) {	
			resourceManagerLoad.remove(destinationAddress);
			jobQueue.add(controlMessage.getJob());
		}

		return null;
	}

	@Override
	public ControlMessage onWriteExceptionThrown(Message message,
			InetSocketAddress destinationAddress, boolean requiresRepsonse) {
		return onReadExceptionThrown(message, destinationAddress);
	}

	/**
	 * onExceptionThrown return null if there is no message to be resent, or return the message to be sent
	 * @param message Original message that was sent before the exception.
	 * @param address Address from the machine where the orignal message was being sent.
	 */
	@Override
	public synchronized ControlMessage onReadExceptionThrown(Message message, InetSocketAddress destinationAddress) {
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage)message;

		if (controlMessage.getType() == ControlMessageType.AddJob) {	
			resourceManagerLoad.remove(destinationAddress);
			jobQueue.add(controlMessage.getJob());
		} 

		if (controlMessage.getType() == ControlMessageType.GSLogJobArrival ||
				controlMessage.getType() == ControlMessageType.GSLogJobCompleted ||
				controlMessage.getType() == ControlMessageType.GSLogJobStarted){
			destinationAddress = controlMessage.getInetAddress();
			if(checkGSFailures(destinationAddress))
				return controlMessage;
		}

		return null;

	}

	private boolean checkGSFailures(InetSocketAddress destinationAddress) {
		gridSchedulersList.replace(
				destinationAddress,
				gridSchedulersList.get(destinationAddress)+1);
		if (gridSchedulersList.get(destinationAddress) > 2) {
			gridSchedulersList.remove(destinationAddress);
			return false;
		}
		return true;
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
				cMessage.setGridSchedulersList(gridSchedulersList.keySet());

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

	//TODO Method called when a message arrives
	private synchronized void logMessage(ControlMessage message) {

		if(message.getType() != ControlMessageType.ReplyLoad && message.getType() != ControlMessageType.GSSendLogEntry ){
			this.logEntry = new LogEntry(message);
			//log.add(this.logEntry);

			ControlMessage msg = new ControlMessage(ControlMessageType.GSSendLogEntry, this.logEntry, hostname, port);
			SynchronizedClientSocket syncClientSocket;
			for(InetSocketAddress address : gridSchedulersList.keySet()) {
				if (address.getHostName() == this.getHostname() && address.getPort() == this.getPort()) continue; //Doe
				System.out.println("Sending logEntry from: "+ this.hostname +":"+ this.port +"to:" + address.toString());
				syncClientSocket = new SynchronizedClientSocket(msg, address, this, timeout);
				syncClientSocket.sendMessage();
			}		
		}	
	}

	/**
	 * Returns the entire history of messages saves on the Grid Scheduler Log
	 */
	public LogEntry[] getFullLog(){

		LogEntry[] log = logger.readOrderedLog();
		try {


			File file = new File(logfilename+"_readable");

			// if file doesnt exist, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			for(LogEntry m : log)
				bw.write(m.toString() + "\n");

			bw.close();

			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		}
		return log;
	}

	/**
	 * Prepares the message passed as argument to be sent to another entity.
	 * 
	 * Some of the messages need to include the local clock in order to let other entities synchronize their own and also have the ability to order the events. 
	 * @param msg
	 * @return the message already with the updated clock.
	 */
	private synchronized ControlMessage prepareMessageToSend(ControlMessage msg) {
		//vClock.incrementClock(identifier);
		msg.setClock(vClock.getClock());
		return msg;
	}

	/**
	 * A message is sent to all the GS in the list of Grid Schedulers in order to synchronize events.
	 * The client blocks until it gets at least a positive response from one of the other GS. 
	 * @param messageToSend
	 */
	private void synchronizeWithAllGS(ControlMessage messageToSend) {

		SyncLog syncLog = new SyncLog();
		SynchronizedClientSocket syncClientSocket;

		ControlMessage msg = prepareMessageToSend(messageToSend);

		for(InetSocketAddress address : gridSchedulersList.keySet()) {
			if (address.getHostString() == this.getHostname() && address.getPort() == this.getPort()) continue;
			syncClientSocket = new SynchronizedClientSocket(msg, address, this, timeout);
			syncClientSocket.sendLogMessage(syncLog);
		}

		//Assume that it always gets a response from at least one of the GS
		if(gridSchedulersList.size() > 1) 
			syncLog.check();

	}

	public static void main(String[] args) {

		String usage = "Usage: GridScheduler <id> <nEntities> <nJobs> <hostname> <port> [<otherGSHostname> <otherGSPort>] [-r]";

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
