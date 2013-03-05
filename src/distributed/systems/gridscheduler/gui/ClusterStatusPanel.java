package distributed.systems.gridscheduler.gui;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.util.List;

import distributed.systems.gridscheduler.model.Cluster;
import distributed.systems.gridscheduler.model.Node;
import distributed.systems.gridscheduler.model.NodeStatus;

/**
 * 
 * A panel that displays information about a Cluster.
 * 
 * @author Niels Brouwers, Boaz Pat-El
 *
 */
public class ClusterStatusPanel extends StatusPanel {
	public final static int padding = 4;
	final static int fontHeight = 12;

	final static int panelWidth = 300;
	final static int statusTextHeight = 60;

	final static int nodeSize = 8;
	final static int nodePadding = 2;
	
	final static Color idleColor = Color.white;
	final static Color busyColor = Color.blue;
	final static Color downColor = Color.red;
	
	/**
	 * Generated serialversionUID
	 */
	private static final long serialVersionUID = -4375781364684663377L;
	
	private Cluster cluster;

	private int colWidth;
	
	public ClusterStatusPanel(Cluster cluster) {
		this.cluster = cluster;
    	
		// calculate the size needed to display the required information
		colWidth = panelWidth / 2;
		int nodeBoxSize = nodeSize + padding;
		double nodeLines = (nodeBoxSize * cluster.getNodeCount()) / (double)(panelWidth - padding);
		int height = statusTextHeight + padding * 2 + 
			((int)Math.ceil(nodeLines)) * nodeBoxSize;
		
		//setSize(new Dimension(panelWidth, height));
		setPreferredSize(new Dimension(panelWidth, height));
	}
	
    protected void paintComponent(Graphics g) {
		// Let UI delegate paint first 
	    // (including background filling, if I'm opaque)
	    super.paintComponent(g);
	    
	    g.drawRect(0,0, getWidth() - 1, getHeight() - 1);

	    g.setColor(Color.gray);
	    g.drawLine(colWidth,padding, colWidth, statusTextHeight - padding);
	    g.setColor(Color.black);
	    
	    // calculate load and availability
	    List <Node> nodes = cluster.getNodes();
	    
	    int nrBusyNodes = 0;
	    int nrDownNodes = 0;
	    for (Node node : nodes) {
	    	if (node.getStatus() == NodeStatus.Busy) nrBusyNodes++;
	    	if (node.getStatus() == NodeStatus.Down) nrDownNodes++;
	    }
	    
	    int load = (int)Math.round( (nrBusyNodes * 100) / (double)nodes.size() );
	    int availability = (int)Math.round( ( (nodes.size() - nrDownNodes) * 100) / (double)nodes.size() );
	    
	    // draw the cluster name and load 
	    int x = padding;
	    int y = padding + fontHeight;
	    
	    g.drawString("Cluster name ", x, y);
	    g.drawString("" + cluster.getName(), x + colWidth, y);
	    y += fontHeight;
	    
	    g.drawString("Nr. of nodes ", x, y);
	    g.drawString("" + cluster.getNodeCount(), x + colWidth, y);
	    y += fontHeight;

	    g.drawString("Load ", x, y);
	    g.drawString("" + load + "%", x + colWidth, y);
	    y += fontHeight;
	    
	    g.drawString("Available ", x, y);
	    g.drawString("" + availability + "%", x + colWidth, y);
	    y += fontHeight;
	    
	    x = padding;
	    y = statusTextHeight + padding;

	    g.setColor(Color.gray);
	    g.drawLine(x, y, x + getWidth() - padding * 2, y);
	    
	    y += padding;
	    
	    for (Node node : nodes) {
	    	// determine color of the nodebox
	    	g.setColor(idleColor);
	    	if (node.getStatus() == NodeStatus.Busy) g.setColor(busyColor);
	    	if (node.getStatus() == NodeStatus.Down) g.setColor(downColor);
	    	
	    	g.fillRect(x, y, nodeSize, nodeSize);

	    	g.setColor(Color.black);
	    	g.drawRect(x, y, nodeSize, nodeSize);
	    	x += nodeSize + padding;
	    	
	    	if (x + nodeSize + padding > getWidth()) {
	    		x = padding;
	    		y += nodeSize + padding;
	    	}
	    	
	    }

    }	

}
