package distributed.systems.gridscheduler.model;

import java.io.File;
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

	private String logfilename = "";
	private LogManager logger;

	private boolean finished = false;

	// job queue
	private ConcurrentLinkedQueue<Job> jobQueue;

	private int identifier;

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
	private static final int TIMEOUT = 3000;

	private ConcurrentHashMap<InetSocketAddress, Integer> gridSchedulersList;

	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<InetSocketAddress, Integer> resourceManagerLoad;
	private ConcurrentHashMap<InetSocketAddress, Integer> rmList;

	private Set<Long> finishedJobs;
	private Thread showInfoThread;

	// polling frequency of the thread that checks if the simulation is over
	private final long checkThreadPollSleep = 1000;
	private final long infoThreadPollSleep = 2000;

	// polling frequency (schedule jobs)
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
		initilizeGridScheduler(id, nEntities, nJobs, hostname, port, false);

		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
	}

	public GridScheduler(int id, int nEntities, int nJobs, String hostname, int port, String otherGSHostname, int otherGSPort, boolean restart) {
		//preconditions
		assert(hostname != null) : "parameter 'hostname' cannot be null";
		assert(port > 0) : "parameter 'port' cannot be less than or equal to 0";
		assert(otherGSHostname != null) : "parameter 'otherGSHostname' cannot be null";
		assert(otherGSPort > 0) : "parameter 'otherGSPort' cannot be less than or equal to 0";
		assert(id >= 0) : "parameter 'id' cannot be less than 0";
		assert(nEntities > 0) : "parameter 'nEntities' should be greater than 0";

		//initialize internal structure (server socket included)
		initilizeGridScheduler(id, nEntities, nJobs, hostname, port, restart);

		SynchronizedClientSocket syncClientSocket;

		// in the case of a restart
		if(restart) {
			// Read file to see last value of the clock (GS)
			logger.readOrderedLog();
			int[][] orderedClocks = logger.getOrderedClocks();
			vClock.setIndexValue(id, (orderedClocks[orderedClocks.length-1][id]));
			System.out.println("INITIAL CLOCK AFTER RESTART: "+vClock.toString());
			logger.writeOrderedLogToTextfile("_restart");
			// IMPORTANT! Free up the memory
			logger.cleanupStructures();
			// Add GS to GS list
			gridSchedulersList.put(new InetSocketAddress(otherGSHostname,otherGSPort),0);
			// Send message to the GS provided in order to log the restart event.
			//TODO Logar o evento antes de enviar a mensagem... (verificar recepcao do ACK)
			ControlMessage cMessage =  new ControlMessage(identifier, ControlMessageType.RestartGS, hostname, port);
			vClock.incrementClock(identifier);
			cMessage.setClock(vClock.getClock());
			LogEntry e = new LogEntry(cMessage);
			logger.writeAsText(e, true);
			// Only one GS in the list at the time.
			syncClientSocket = new SynchronizedClientSocket(cMessage, new InetSocketAddress(otherGSHostname, otherGSPort),this, TIMEOUT);
			syncClientSocket.sendMessage();
		}

		//in the case where another GS was provided, query that GS for the complete GS list
		ControlMessage cMessage =  new ControlMessage(identifier, ControlMessageType.GSRequestsGSList, hostname, port);

		//send the message querying the other GS
		syncClientSocket = new SynchronizedClientSocket(cMessage, new InetSocketAddress(otherGSHostname, otherGSPort),this, TIMEOUT);
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

				// With this flag, this GS wont care about messages sent from others.
				finished = true;
				// Stop running threads.
				pollingThread.interrupt();
				showInfoThread.interrupt();

				System.out.println("GS <"+hostname+":"+port+">: Simulation is over!");
				ControlMessage message;
				SynchronizedClientSocket syncClientSocket;

				// Send message to all RM's
				for(InetSocketAddress address : resourceManagerLoad.keySet()) {
					message = new ControlMessage(identifier, ControlMessageType.SimulationOver,hostname, port);
					syncClientSocket = new SynchronizedClientSocket(message, address, handler, TIMEOUT);
					syncClientSocket.sendMessageWithoutResponse();
				}

				// Send to all GS's
				for(InetSocketAddress address : gridSchedulersList.keySet()) {
					message = new ControlMessage(identifier, ControlMessageType.SimulationOver,hostname, port);
					syncClientSocket = new SynchronizedClientSocket(message, address, handler, TIMEOUT);
					syncClientSocket.sendMessageWithoutResponse();
				}

				System.out.println("Shutting down in 10 seconds...");

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println("Writting log file in text format...");
				logger.readOrderedLog();
				logger.writeOrderedLogToTextfile("_final");
				logger.cleanupStructures();
				File f = new File(logger.getFilename());
				f.delete();
				System.out.println("Shutting down now!...");

				System.exit(0);

			}
		}).start();
	}

	private void initilizeGridScheduler(int id, int nEntities, int nJobs, String hostname, 
			int port, boolean restart){

		this.hostname = hostname;
		this.port = port;
		this.identifier = id;
		this.nJobs = nJobs;
		this.logfilename += "GS_" + id +".log";
		this.handler = this;

		//Cleanup previous log file.
		if(!restart) {
			File file = new File (logfilename);
			file.delete();
		}

		this.logger = new LogManager(logfilename);

		gridSchedulersList = new ConcurrentHashMap<InetSocketAddress,Integer>();
		gridSchedulersList.put(new InetSocketAddress(hostname, port),0);
		finishedJobs = new HashSet<Long>();

		this.resourceManagerLoad = new ConcurrentHashMap<InetSocketAddress, Integer>();
		this.rmList = new ConcurrentHashMap<InetSocketAddress, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		//this.log = new ArrayList<LogEntry>();	
		vClock = new VectorialClock(nEntities);

		syncSocket = new SynchronizedSocket(hostname, port);
		syncSocket.addMessageReceivedHandler(this);

		launchCheckThread();

		//TODO Remove if we don't want to see updates
		showInfoThread = new Thread(new Runnable() {
			public void run() {
				while(true) {
					int i = 0;
					String print = "";
					for(InetSocketAddress rm : resourceManagerLoad.keySet()) {
						print += String.format(rm.getHostName()+":"+rm.getPort()+"[%3d]  ",resourceManagerLoad.get(rm));
						i++;
						if(i%6==0) {
							i=0;
							print += "\n";
						}
					}

					int nbThreads =  Thread.getAllStackTraces().keySet().size();
					int nbRunning = 0;
					for (Thread t : Thread.getAllStackTraces().keySet()) {
						if (t.getState()==Thread.State.RUNNABLE) nbRunning++;
					}
					System.out.println(print+"\n Jobs finished: "+jobsFinished+"\n"+gridSchedulersList);
					System.out.println(print+"1. Active Threads: "+Thread.activeCount());
					System.out.println(print+"2. Active Threads: "+nbRunning);
					System.out.println(print+"3. Total Number of Threads: "+nbThreads);

					try {
						Thread.sleep(infoThreadPollSleep);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
				}
			}
		});
		showInfoThread.start();

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
		ControlMessage msg = null;
		ControlMessage msgOpt = null;
		ControlMessage msgLog = null;
		int[] tempVC = null;

		if (controlMessage.getType() == ControlMessageType.ReplyLoad) {
			resourceManagerLoad.put(controlMessage.getInetAddress(),controlMessage.getLoad());
			rmList.put(controlMessage.getInetAddress(), 0);
		}

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
			msg = new ControlMessage(identifier, ControlMessageType.ReplyGSList, hostname, port);
			msg.setGridSchedulersList(gridSchedulersList.keySet());
			return msg;
		}		

		if (controlMessage.getType() == ControlMessageType.GSRequestsGSList) {
			gridSchedulersList.put(controlMessage.getInetAddress(),0);
			msg = new ControlMessage(identifier, ControlMessageType.ReplyGSList, hostname, port);
			msg.setGridSchedulersList(gridSchedulersList.keySet());	
			for(InetSocketAddress address : gridSchedulersList.keySet()) {
				if (address.getPort() == this.getPort() && address.getHostName() == this.getHostname()) continue;
				if (address.equals(controlMessage.getInetAddress())) continue;
				syncClientSocket = new SynchronizedClientSocket(msg, address, this, TIMEOUT);
				syncClientSocket.sendMessageWithoutResponse();
			}			
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogRestartRM) {
			gridSchedulersList.put(controlMessage.getInetAddress(), 0);
			tempVC = vClock.updateClock(controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.GSLogRestartRMAck, hostname, port,tempVC);
			logger.writeAsText(new LogEntry(controlMessage),true);
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.RestartRM) {
			tempVC = vClock.updateClock(controlMessage.getClock());
			msgLog = new ControlMessage(identifier, ControlMessageType.GSLogRestartRM, hostname, port, controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.RestartRMAck, hostname, port, tempVC);
			logger.writeAsText(new LogEntry(controlMessage),true);
			synchronizeWithAllGS(msgLog);
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.RestartGS) {
			gridSchedulersList.put(controlMessage.getInetAddress(),0);
			tempVC = vClock.updateClock(controlMessage.getClock());
			msgLog = new ControlMessage(identifier, ControlMessageType.GSLogRestartGS, hostname, port, controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.RestartGSAck, hostname, port, tempVC);
			logger.writeAsText(new LogEntry(controlMessage),true);
			synchronizeWithAllGS(msgLog);
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.RestartGSAck) {
			vClock.updateClock(controlMessage.getClock());
			logger.writeAsText(new LogEntry(controlMessage),true);
			return null;
		}

		// resource manager wants to offload a job to us
		// there is no need to log because the RM is waiting for an ACK.
		// The ACK message will only be sent when the Job is delegated to another RM.
		if (controlMessage.getType() == ControlMessageType.AddJob) {
			// Tries to add the RM to the list if this was not there already.
			rmList.put(controlMessage.getInetAddress(), 0);
			if(!resourceManagerLoad.containsKey(controlMessage.getInetAddress())) {
				resourceManagerLoad.put(controlMessage.getInetAddress(), 0);
			}
			vClock.updateClock(controlMessage.getClock());
			jobQueue.add(controlMessage.getJob());
		}

		if (controlMessage.getType() == ControlMessageType.JobArrival) {			
			rmList.put(controlMessage.getInetAddress(), 0);

			tempVC = vClock.updateClock(controlMessage.getClock());

			msgLog = new ControlMessage(controlMessage.getId(), ControlMessageType.GSLogJobArrival, controlMessage.getJob(), hostname, port, controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.JobArrivalAck, hostname, port, tempVC);

			logger.writeAsText(new LogEntry(controlMessage),true);
			synchronizeWithAllGS(msgLog);

			msgOpt = new ControlMessage(identifier, ControlMessageType.AddJobAck, controlMessage.getJob(), hostname, port);
			msgOpt.setClock(tempVC);

			syncClientSocket = new SynchronizedClientSocket(msgOpt, controlMessage.getJob().getOriginalRM(), this, TIMEOUT);
			syncClientSocket.sendMessageWithoutResponse();			

			//TODO Enviar esta mensagem antes de enviar o AddJobAck??
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogJobArrival) {
			gridSchedulersList.put(controlMessage.getInetAddress(), 0);
			tempVC = vClock.updateClock(controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.GSLogJobArrivalAck, hostname, port,tempVC);
			logger.writeAsText(new LogEntry(controlMessage),true);
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.JobStarted) {
			rmList.put(controlMessage.getInetAddress(), 0);
			tempVC = vClock.updateClock(controlMessage.getClock());
			msgLog = new ControlMessage(controlMessage.getId(), ControlMessageType.GSLogJobStarted, controlMessage.getJob(), hostname, port, controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.JobStartedAck, hostname, port, tempVC);
			logger.writeAsText(new LogEntry(controlMessage),true);
			synchronizeWithAllGS(msgLog);
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogJobStarted) {
			gridSchedulersList.put(controlMessage.getInetAddress(), 0);
			tempVC = vClock.updateClock(controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.GSLogJobStartedAck, hostname, port,tempVC);
			logger.writeAsText(new LogEntry(controlMessage),true);
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.JobCompleted) {
			rmList.put(controlMessage.getInetAddress(), 0);
			tempVC = vClock.updateClock(controlMessage.getClock());
			msgLog = new ControlMessage(controlMessage.getId(), ControlMessageType.GSLogJobCompleted, controlMessage.getJob(),  hostname, port, controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.JobCompletedAck, hostname, port, tempVC);
			logger.writeAsText(new LogEntry(controlMessage),true);
			synchronizeWithAllGS(msgLog);
			synchronized (this) {
				if(finishedJobs.add(controlMessage.getJob().getId())) {
					jobsFinished++;
				}
			}
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogJobCompleted) {
			gridSchedulersList.put(controlMessage.getInetAddress(), 0);
			tempVC = vClock.updateClock(controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.GSLogJobCompletedAck, controlMessage.getJob(), hostname, port, tempVC);
			logger.writeAsText(new LogEntry(controlMessage),true);
			synchronized (this) {
				if(finishedJobs.add(controlMessage.getJob().getId())) {
					jobsFinished++;
				}
			}
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.RestartGS) {
			vClock.updateClock(controlMessage.getClock());
			msgLog = new ControlMessage(identifier, ControlMessageType.GSLogRestartGS, controlMessage.getJob(), hostname, port, controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.RestartGSAck, hostname, port, vClock.getClock());
			logger.writeAsText(new LogEntry(controlMessage),true);
			synchronizeWithAllGS(msgLog);
			return msg;
		}

		if (controlMessage.getType() == ControlMessageType.GSLogRestartGS) {
			gridSchedulersList.put(controlMessage.getInetAddress(), 0);
			vClock.updateClock(controlMessage.getClock());
			msg = new ControlMessage(identifier, ControlMessageType.GSLogRestartGSAck, hostname, port, controlMessage.getClock());
			logger.writeAsText(new LogEntry(controlMessage),true);
			return msg;
		}

		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin) {
			resourceManagerLoad.put(controlMessage.getInetAddress(), 0);
			rmList.put(controlMessage.getInetAddress(), 0);
			return new ControlMessage(identifier, ControlMessageType.ResourceManagerJoinAck, hostname, port, vClock.updateClock(controlMessage.getClock()));
		}

		if (controlMessage.getType() == ControlMessageType.SimulationOver) {
			if(!finished) {
				synchronized (this) {
					finished = true;
					System.out.println("Shutting down in 2 seconds...");
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
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
		ControlMessage controlMessage = (ControlMessage)message;
		if (controlMessage.getType() == ControlMessageType.GSLogJobArrival ||
				controlMessage.getType() == ControlMessageType.GSLogJobCompleted ||
				controlMessage.getType() == ControlMessageType.GSLogJobStarted){

			if(checkGSFailures(destinationAddress)) {
				SynchronizedClientSocket client = new SynchronizedClientSocket(controlMessage, destinationAddress, this, TIMEOUT);
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
			if(checkRMFailures(destinationAddress)) {
				resourceManagerLoad.remove(destinationAddress);
				jobQueue.add(controlMessage.getJob());
			}
		} 

		if (controlMessage.getType() == ControlMessageType.RequestLoad) {
			checkRMFailures(destinationAddress);
		}

		if (controlMessage.getType() == ControlMessageType.GSLogJobArrival ||
				controlMessage.getType() == ControlMessageType.GSLogJobCompleted ||
				controlMessage.getType() == ControlMessageType.GSLogJobStarted){
			if(checkGSFailures(destinationAddress))
				return controlMessage;
		}

		if (controlMessage.getType() == ControlMessageType.RestartGS) {
			return controlMessage;
		}

		return null;

	}

	private synchronized boolean checkRMFailures(InetSocketAddress destinationAddress) {
		Integer failures = rmList.get(destinationAddress);
		if(failures != null) {
			if (failures > 1) {
				rmList.remove(destinationAddress);
				resourceManagerLoad.remove(destinationAddress);
				return false;
			}
			else {
				rmList.replace(destinationAddress,failures+1);
			}
		}
		return false;
	}

	private synchronized boolean checkGSFailures(InetSocketAddress destinationAddress) {
		Integer failures = gridSchedulersList.get(destinationAddress);
		if(failures != null) {
			if (failures > 1) {
				gridSchedulersList.remove(destinationAddress);
				return false;
			}
			else {
				gridSchedulersList.replace(destinationAddress,failures+1);
			}
		}
		return false;
	}

	// finds the least loaded resource manager and returns its url
	private InetSocketAddress getLeastLoadedRM(InetSocketAddress ignoreAddress) {
		InetSocketAddress ret = null; 
		int maxFreeNodes = Integer.MIN_VALUE;
		Set<InetSocketAddress> rm = new HashSet<InetSocketAddress>();
		// loop over all resource managers, and pick the one with the lowest load
		synchronized (resourceManagerLoad) {
			for (InetSocketAddress key : resourceManagerLoad.keySet())
			{
				//				if(key.equals(ignoreAddress))
				//					continue;
				if (resourceManagerLoad.get(key) > maxFreeNodes)
				{
					maxFreeNodes = resourceManagerLoad.get(key);
					rm.clear();
					rm.add(key);
				} else if(resourceManagerLoad.get(key) == maxFreeNodes) {
					rm.add(key);
				}
			}
		}
		if(rm.size()>0) {
			ret = (InetSocketAddress)rm.toArray()[(int)(Math.random() * ((rm.size()-1) + 1))];
		}
		return ret;		
	}

	/**
	 * Randomly selects one GS from the poll of GS's
	 * @return the address of the chosen GS
	 *//*
	private InetSocketAddress getRandomGS() {
		return (InetSocketAddress)gridSchedulersList.keySet().toArray()[(int)(Math.random() * ((gridSchedulersList.size()-1) + 1))];
	}*/

	/**
	 * Polling thread runner. This thread polls each resource manager in turn to get its load,
	 * then offloads any job in the waiting queue to that resource manager
	 */
	public void run() {

		SynchronizedClientSocket syncClientSocket = null;
		int freeNodes = 0;
		int[] tempVC = null;

		while (running) {
			// send a message to each resource manager, requesting its load
			for (InetSocketAddress inetAdd : resourceManagerLoad.keySet())
			{
				ControlMessage cMessage = new ControlMessage(identifier, ControlMessageType.RequestLoad);
				cMessage.setHostname(this.getHostname());
				cMessage.setPort(this.getPort());
				cMessage.setGridSchedulersList(gridSchedulersList.keySet());
				//syncSocket.sendMessage(cMessage, inetAdd);
				syncClientSocket = new SynchronizedClientSocket(cMessage, inetAdd, this, TIMEOUT);
				syncClientSocket.sendMessage();
			}

			// schedule waiting messages to the different clusters
			for (Job job : jobQueue)
			{
				InetSocketAddress leastLoadedRM =  getLeastLoadedRM(job.getOriginalRM());

				if (leastLoadedRM!=null) {

					tempVC = vClock.incrementClock(identifier);

					ControlMessage cMessage ;
					cMessage = new ControlMessage(identifier, ControlMessageType.AddJob, job,this.getHostname(), this.getPort());					
					cMessage.setClock(tempVC);

					jobQueue.remove(job);

					syncClientSocket = new SynchronizedClientSocket(cMessage, leastLoadedRM, this, TIMEOUT);
					syncClientSocket.sendMessageWithoutResponse();

					// decrease the number of free nodes (because we just added a job)
					freeNodes = resourceManagerLoad.get(leastLoadedRM);
					resourceManagerLoad.put(leastLoadedRM, freeNodes-1);

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

	/**
	 * Returns the entire history of messages saves on the Grid Scheduler Log
	 */
	//	public LogEntry[] getFullLog(){
	//
	//		LogEntry[] log = logger.readOrderedLog();
	//		try {
	//
	//
	//			File file = new File(logfilename+"_readable");
	//
	//			// if file doesnt exist, then create it
	//			if (!file.exists()) {
	//				file.createNewFile();
	//			}
	//
	//			FileWriter fw = new FileWriter(file.getAbsoluteFile());
	//			BufferedWriter bw = new BufferedWriter(fw);
	//
	//			for(LogEntry m : log)
	//				bw.write(m.toString() + "\n");
	//
	//			bw.close();
	//
	//		} catch (IOException e) {
	//			e.printStackTrace();
	//		}
	//		return log;
	//	}

	/**
	 * A message is sent to all the GS in the list of Grid Schedulers in order to synchronize events.
	 * The client blocks until it gets at least a positive response from one of the other GS. 
	 * @param messageToSend
	 */
	private void synchronizeWithAllGS(ControlMessage messageToSend) {

		SyncLog syncLog = new SyncLog();
		SynchronizedClientSocket syncClientSocket;

		for(InetSocketAddress address : gridSchedulersList.keySet()) {
			if (address.getHostName() == this.getHostname() && address.getPort() == this.getPort()) continue;
			syncClientSocket = new SynchronizedClientSocket(messageToSend, address, this, TIMEOUT);
			syncClientSocket.sendLogMessage(syncLog);
		}

		//Assume that it always gets a response from at least one of the GS
		if(gridSchedulersList.size() > 1) 
			syncLog.check();

	}

	public static void main(String[] args) {

		String usage = "Usage: GridScheduler <id> <nEntities> <nJobs> <hostname> <port> [<otherGSHostname> <otherGSPort> [-r]]";

		if(args.length != 5 && args.length != 7 && args.length != 8) {
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
			} else if (args.length == 7) {
				new GridScheduler(
						Integer.parseInt(args[0]), 
						Integer.parseInt(args[1]),
						Integer.parseInt(args[2]),
						args[3], 
						Integer.parseInt(args[4]),
						args[5], 
						Integer.parseInt(args[6]),
						false);
			}  else if (args.length == 8) {
				if(args[7].equals("-r")) {
					new GridScheduler(
							Integer.parseInt(args[0]), 
							Integer.parseInt(args[1]),
							Integer.parseInt(args[2]),
							args[3], 
							Integer.parseInt(args[4]),
							args[5], 
							Integer.parseInt(args[6]),
							true);
				} else {
					System.out.println("Wrong flag usage!\n"+usage);
					System.exit(1);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Invalid parameter types used!\n"+usage);
			System.exit(1);
		}
	}

}
