package distributed.systems.core;

import java.io.Serializable;

import com.sun.swing.internal.plaf.synth.resources.synth;

/**
 * Tipo de dados que implementa um regio vectorial.
 * @author mX
 */
public class VectorialClock implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = -4298646294358826625L;
		/**
     * Relgio Vectorial.
     */
    private int[] clock;

    public VectorialClock(int nEntities) {
        clock = new int[nEntities];
        for (int i = 0; i < nEntities; i++) {
            clock[i] = 0;
        }
    }

    /**
     * 
     * @return clock
     */
    public int[] getClock() {
        return clock;
    }

    /**
     * Increment
     * @param id 
     */
    public synchronized void incrementClock(int id) {
        clock[id]++;
    }

    /**
     * Faz o update do clock comparando-o com o externalClock dado.
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
