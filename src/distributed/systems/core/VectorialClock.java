package distributed.systems.core;

import java.io.Serializable;

/**
 * Tipo de dados que implementa um relógio vectorial.
 * @author mX
 */
public class VectorialClock implements Serializable {

    /**
     * Relógio Vectorial.
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
     * Inscrementa o clock na posição id
     * @param id 
     */
    public void incrementClock(int id) {
        clock[id]++;
    }

    /**
     * Faz o update do clock comparando-o com o externalClock dado.
     * @param externalClock
     * @param id 
     */
    public void updateClock(int[] externalClock, int id) {
        for (int i = 0; i < clock.length; i++) {
            if (externalClock[i] > clock[i]) {
                clock[i] = externalClock[i];
            }
        }

    }
    
   
}
