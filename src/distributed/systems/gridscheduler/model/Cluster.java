package distributed.systems.gridscheduler.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import distributed.systems.core.LogEntry;

/**
 * 
 * The Cluster class represents a single cluster in the virtual grid system. It consists of a 
 * collection of nodes and a resource manager. 
 * 
 * @author Niels Brouwers
 *
 */
public class Cluster implements Runnable {

	private List <Node> nodes;
	private ResourceManager resourceManager;
	private String hostname;
	private int port;
	private int id;
	private int nJobsToExecute;
	private boolean restart;

	private Set<Long> finishedJobs;

	// polling frequency, 10hz
	private long pollSleep = 100;

	// polling thread
	private Thread pollingThread;
	private boolean running;

	/**
	 * Creates a new Cluster, with a number of nodes and a resource manager
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B> 
	 * <DD>parameter <CODE>name</CODE> cannot be null<br>
	 * <DD>parameter <CODE>gridSchedulerHostname</CODE> cannot be null<br>
	 * <DD>parameter <CODE>gridSchedulerPort</CODE> cannot be inferior or equal to 0<br>
	 * <DD>parameter <CODE>nrNodes</code> must be greater than 0
	 * </DL>
	 * @param name the name of this cluster
	 * @param nrNodes the number of nodes in this cluster
	 */
	public Cluster(final int id, int nEntities, int nodeCount, final int nJobsToExecute, 
			String hostname, int port, String gridSchedulerHostname, 
			int gridSchedulerPort, boolean restart) {
		// Preconditions
		assert(hostname != null) : "parameter 'hostname' cannot be null";
		assert(port > 0) : "parameter 'port' cannot be inferior or equal to 0";
		assert(gridSchedulerHostname != null) : "parameter 'gridSchedulerHostname' cannot be null";
		assert(gridSchedulerPort > 0) : "parameter 'gridSchedulerPort' cannot be inferior or equal to 0";
		assert(nodeCount > 0) : "parameter 'nodeCount' cannot be smaller or equal to zero";

		// Initialize members
		this.id = id;
		this.hostname = hostname;
		this.port = port;
		this.nJobsToExecute = nJobsToExecute;
		this.restart = restart;
		this.nodes = new ArrayList<Node>(nodeCount);

		// Initialize the resource manager for this cluster
		resourceManager = new ResourceManager(id, nEntities,this, restart);
		resourceManager.connectToGridScheduler(gridSchedulerHostname,gridSchedulerPort);

		// Initialize the nodes 
		for (int i = 0; i < nodeCount; i++) {
			Node n = new Node();
			// Make nodes report their status to the resource manager
			n.addNodeEventHandler(resourceManager);
			nodes.add(n);
		}

		//TODO Dentro desta thread, tem de verificar se restart = true, e se sim, ir ao log, 
		//buscar os trabalhos j‡ gerados para apenas gerar os que ainda n‹o foram enviados a um grid Scheduler
		if(restart) {

			String aux = null;
			LogEntry[] log = this.resourceManager.getFullLog();

			finishedJobs = new HashSet<Long>();
			//TODO Escolher os ID's dos trabalhos que já não precisam de ser gerados e colocar num Set
			//  Critérios: 
			//    - Jobs que tenham sido delegados com sucesso (JobAddAck deve logar este evento)
			//    - Jobs que tenham completado nesta máquina (um evento é logado quando acontece)
			// Estes dois eventos devem ser diferentes - verificar!
			for(LogEntry l : log) {
				aux = l.getEvent();
				if(aux.equals("JOB_COMPLETED") || aux.equals("JOB_SENT"))
					finishedJobs.add(l.getJob().getId());
			}

			// Launch the thread with th JobID verification upon generation.
			Thread createJobs = new Thread(new Runnable() {
				public void run() {
					int jobId = id*100000;
					for(int i = 0; i < nJobsToExecute; i++) {
						jobId++;
						if(finishedJobs.contains(jobId))
							continue;
						Job job = new Job(8000 + (int)(Math.random() * 5000), jobId);
						getResourceManager().addJob(job);
						// Sleep a while before creating a new job
						try {
							Thread.sleep(20L);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
					}
				}
			});
			createJobs.start();
		}
		// Launch the thread normallly.
		else {
			Thread createJobs = new Thread(new Runnable() {
				public void run() {
					int jobId = id*100000;
					for(int i = 0; i < nJobsToExecute; i++) {
						Job job = new Job(8000 + (int)(Math.random() * 5000), jobId++);
						getResourceManager().addJob(job);
						// Sleep a while before creating a new job
						try {
							Thread.sleep(20L);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
					}
				}
			});
			createJobs.start();
		}

		// Start the polling thread
		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();

	}

	/**
	 * Returns the number of nodes in this cluster. 
	 * @return the number of nodes in this cluster
	 */
	public int getNodeCount() {
		return nodes.size();
	}

	/**
	 * Returns the resource manager object for this cluster.
	 * @return the resource manager object for this cluster
	 */
	public ResourceManager getResourceManager() {
		return resourceManager;
	}

	/**
	 * Returns the ID of the cluster
	 * @return the ID of the cluster
	 */
	public int getID() {
		return id;
	}

	/**
	 * Returns the hostname of the cluster
	 * @return the hostname of the cluster
	 */
	public String getName() {
		return hostname;
	}

	/**
	 * Returns the port of the cluster
	 * @return the port of the cluster
	 */
	public int getPort() {
		return this.port;
	}

	/**
	 * Returns the nodes inside the cluster as an array.
	 * @return an array of Node objects
	 */
	public List<Node> getNodes() {
		return nodes;
	}

	/**
	 * Finds a free node and returns it. If no free node can be found, the method returns null.
	 * @return a free Node object, or null if no such node can be found. 
	 */
	public Node getFreeNode() {
		// Find a free node among the nodes in our cluster
		for (Node node : nodes)
			if (node.getStatus() == NodeStatus.Idle) return node;

		// if we haven't returned from the function here, we haven't found a suitable node
		// so we just return null
		return null;
	}

	/**
	 * Polling thread runner. This function polls each node in the system repeatedly. Polling
	 * is needed to make each node check its internal state - whether a running job is 
	 * finished for instance.
	 */
	public void run() {

		while (running) {
			// poll the nodes
			for (Node node : nodes)
				node.poll();

			// sleep
			try {
				Thread.sleep(pollSleep);
			} catch (InterruptedException ex) {
				assert(false) : "Cluster poll thread was interrupted";
			}

		}

	}

	/**
	 * Stops the polling thread. This must be called explicitly to make sure the program
	 * terminates cleanly.
	 */
	public void stopPollThread() {
		running = false;
		try {
			pollingThread.join();
		} catch (InterruptedException ex) {
			assert(false) : "Cluster stopPollThread was interrupted";
		}

	}

	public static void main(String[] args) {

		String usage = "Usage: Cluster <id> <nEntities> <nNodes> <nJobsToExecute> <hostname> <port> <GSHostname> <GSPort> [-r]";

		if(args.length != 8 && args.length != 9) {
			System.out.println(usage);
			System.exit(1);
		}

		if(args.length==9) {
			if(args[8].equals("-r")) {
				try {
					System.out.println("Launching cluster in RESTART mode.");
					new Cluster(
							Integer.parseInt(args[0]), 
							Integer.parseInt(args[1]),
							Integer.parseInt(args[2]),
							Integer.parseInt(args[3]),
							args[4], 
							Integer.parseInt(args[5]), 
							args[6], 
							Integer.parseInt(args[7]),
							true);
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(usage);
					System.exit(1);
				}
			} else {
				System.out.println(usage);
				System.exit(1);
			}
		}

		else if(args.length==8) {
			try {
				System.out.println("Launching cluster in NORMAL mode.");
				new Cluster(
						Integer.parseInt(args[0]), 
						Integer.parseInt(args[1]),
						Integer.parseInt(args[2]),
						Integer.parseInt(args[3]),
						args[4], 
						Integer.parseInt(args[5]), 
						args[6], 
						Integer.parseInt(args[7]),
						false);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(usage);
				System.exit(1);
			}
		}
	}


}
