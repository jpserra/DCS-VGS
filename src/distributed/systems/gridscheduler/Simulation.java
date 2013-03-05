package distributed.systems.gridscheduler;

import javax.swing.JFrame;

import distributed.systems.gridscheduler.gui.ClusterStatusPanel;
import distributed.systems.gridscheduler.gui.GridSchedulerPanel;
import distributed.systems.gridscheduler.model.Cluster;
import distributed.systems.gridscheduler.model.GridScheduler;
import distributed.systems.gridscheduler.model.Job;

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
		GridScheduler scheduler;
		
		// Setup the model. Create a grid scheduler and a set of clusters.
		scheduler = new GridScheduler("scheduler1");

		// Create a new gridscheduler panel so we can monitor our components
		gridSchedulerPanel = new GridSchedulerPanel(scheduler);
		gridSchedulerPanel.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		// Create the clusters and nods
		clusters = new Cluster[nrClusters];
		for (int i = 0; i < nrClusters; i++) {
			clusters[i] = new Cluster("cluster" + i, scheduler.getUrl(), nrNodes); 
			
			// Now create a cluster status panel for each cluster inside this gridscheduler
			ClusterStatusPanel clusterReporter = new ClusterStatusPanel(clusters[i]);
			gridSchedulerPanel.addStatusPanel(clusterReporter);
		}
		
		// Open the gridscheduler panel
		gridSchedulerPanel.start();
		
		// Run the simulation
		Thread runThread = new Thread(this);
		runThread.run(); // This method only returns after the simulation has ended
		
		// Now perform the cleanup
		
		// Stop clusters
		for (Cluster cluster : clusters)
			cluster.stopPollThread();
		
		// Stop grid scheduler
		scheduler.stopPollThread();
	}

	/**
	 * The main run thread of the simulation. You can tweak or change this code to produce
	 * different simulation scenarios. 
	 */
	public void run() {
		long jobId = 0;
		// Do not stop the simulation as long as the gridscheduler panel remains open
		while (gridSchedulerPanel.isVisible()) {
			// Add a new job to the system that take up random time
			Job job = new Job(8000 + (int)(Math.random() * 5000), jobId++);
			clusters[0].getResourceManager().addJob(job);
			
			try {
				// Sleep a while before creating a new job
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				assert(false) : "Simulation runtread was interrupted";
			}
			
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
