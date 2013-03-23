package distributed.systems.gridscheduler;

import javax.swing.JFrame;
import distributed.systems.core.LogEntry;
import distributed.systems.core.LogManager;
import distributed.systems.gridscheduler.gui.ClusterStatusPanel;
import distributed.systems.gridscheduler.gui.GridSchedulerPanel;
import distributed.systems.gridscheduler.model.Cluster;
import distributed.systems.gridscheduler.model.GridScheduler;
import distributed.systems.gridscheduler.model.Job;
import distributed.systems.gridscheduler.model.Node;
import distributed.systems.gridscheduler.model.NodeStatus;

/**
 *
 * The Simulation class is an example of a grid computation scenario. Every 100 milliseconds 
 * a new job is added to first cluster. As this cluster is swarmed with jobs, it offloads
 * some of them to the grid scheduler, wich in turn passes them to the other clusters.
 * 
 * @author Niels Brouwers, Boaz Pat-El
 */
public class Simulation implements Runnable {
	// Number of clusters in the simulation
	private final static int nrClusters = 5;
	private final static int nrGS= 2;

	// Number of nodes per cluster in the simulation
	private final static int nrNodes = 50;

	// Simulation components
	Cluster clusters[];

	GridSchedulerPanel gridSchedulerPanel;

	/**
	 * Constructs a new simulation object. Study this code to see how to set up your own
	 * simulation.
	 */
	public Simulation() {

		GridScheduler gs1, gs2;

		int nEntities = nrClusters + nrGS;
		// Setup the model. Create a grid scheduler and a set of clusters.
		gs1 = new GridScheduler( 0, nEntities,"localhost", 50000);
		gs2 = new GridScheduler( 1, nEntities, "localhost", 50001, "localhost", 50000);

		// Create a new gridscheduler panel so we can monitor our components
		gridSchedulerPanel = new GridSchedulerPanel(gs1,gs2);
		gridSchedulerPanel.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		
		// Create the clusters and nods
		System.out.println("Init Clusters");
		clusters = new Cluster[nrClusters];
		for (int i = 0; i < nrClusters; i++) {
			clusters[i] = new Cluster(i+2, nEntities, "localhost", 50050 + i, gs1.getUrl(), gs1.getPort(), nrNodes); 

			// Now create a cluster status panel for each cluster inside this gridscheduler
			ClusterStatusPanel clusterReporter = new ClusterStatusPanel(clusters[i]);
			gridSchedulerPanel.addStatusPanel(clusterReporter);
		}

		// Open the gridscheduler panel
		gridSchedulerPanel.start();

		// Run the simulation
		Thread runThread = new Thread(this);
		runThread.run(); // This method only returns after the simulation has ended


		//Print log
		System.out.println("Simulation Finished, printing Log... GS1");


		for(LogEntry e: gs1.getFullLog())
			System.out.println(e.toString());

		System.out.println("Simulation Finished, printing Log... GS2");


		for(LogEntry e: gs2.getFullLog())
			System.out.println(e.toString());

		// Now perform the cleanup

		// Stop clusters
		for (Cluster cluster : clusters){
			System.out.println("LOG DO RM Dum CLUSTER");
			for(Object j: LogManager.readFromBinaryFile(cluster.getResourceManager().getLogFileName())){
				System.out.println(j.toString());
			}

			cluster.stopPollThread();
		}

		// Stop grid scheduler
		gs1.stopPollThread();

		gridSchedulerPanel.dispose();

		System.exit(1);
	}

	/**
	 * The main run thread of the simulation. You can tweak or change this code to produce
	 * different simulation scenarios. 
	 */
	public void run() {
		long jobId = 0;
		//to randomize the job attribution to clusters
		int cId = 0;
		// Do not stop the simulation as long as the gridscheduler panel remains open
		while (gridSchedulerPanel.isVisible()) {
			// Add a new job to the system that take up random time
			Job job = new Job(8000 + (int)(Math.random() * 5000), jobId++);
			cId = (int)(Math.random() * (nrClusters));
			clusters[0].getResourceManager().addJob(job);

			try {
				// Sleep a while before creating a new job
				Thread.sleep(20L);
				//Limit number of jobs
				if (jobId == 200) {
					boolean finished = false;
					while(!finished){
outerLoop: for( int i= 0; i< nrClusters; i++){
							if(clusters[i].getResourceManager().getWaitingJob() != null ) break outerLoop;
							for(Node n: clusters[i].getNodes()){
								if (n.getStatus() == NodeStatus.Busy){
									break outerLoop;						
								}
							}
							finished = true;
						}

					}
					return;
				}

			} catch (InterruptedException e) {
				assert(false) : "Simulation runtread was interrupted";
			}
			;
		}
	}

	/**
	 * Application entry point.
	 * @param args application parameters
	 */
	public static void main(String[] args) {
		// Create and run the simulation
		new Simulation();
	}

}
