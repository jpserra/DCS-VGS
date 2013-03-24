package distributed.systems.core;

import java.io.Serializable;

/**
 * Class that represents a Vectorial Clock
 */
public class VectorialClock implements Serializable {

	private static final long serialVersionUID = -4298646294358826625L;

    private int[] clock;

    public VectorialClock(int nEntities) {
        clock = new int[nEntities];
        for (int i = 0; i < nEntities; i++) {
            clock[i] = 0;
        }
    }

    /**
     * Returns the clock.
     * @return clock
     */
    public int[] getClock() {
        return clock;
    }

    /**
     * Increment the clock at a specified ID by one unit
     * @param id 
     */
    public synchronized void incrementClock(int id) {
        clock[id]++;
    }

    /**
     * Updates the clock using an external clock as reference.
     * @param externalClock
     * @param id 
     */
    public synchronized void updateClock(int[] externalClock, int id) {
        for (int i = 0; i < clock.length; i++) {
            if (externalClock[i] > clock[i]) {
                clock[i] = externalClock[i];
            }
        }
    }
    
}
