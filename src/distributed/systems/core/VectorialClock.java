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
	
	public VectorialClock(int[] clock) {
		this.clock = clock;
	}

	/**
	 * Returns the clock.
	 * @return clock
	 */
	public int[] getClock() {
		return clock.clone();
	}

	/**
	 * Increment the clock at a specified ID by one unit
	 * @param id 
	 */
	public synchronized int[] incrementClock(int id) {
		clock[id]++;
		return clock;
	}

	/**
	 * Updates the clock using an external clock as reference.
	 * @param externalClock
	 * @param id 
	 */
	public synchronized int[] updateClock(int[] externalClock) {
		for (int i = 0; i < clock.length; i++) {
			if (externalClock[i] > clock[i]) {
				clock[i] = externalClock[i];
			}
		}
		return clock.clone();
	}

	public synchronized int[] updateClockID(int[] externalClock, int id) {
		for (int i = 0; i < clock.length; i++) {
			if(id!=i) {
				if (externalClock[i] > clock[i]) {
					clock[i] = externalClock[i];
				}
			} else {
				clock[i] = externalClock[i];
			}
		}
		return clock.clone();
	}

	public void setClock(int[] externalClock) {
		for (int i = 0; i < clock.length; i++) {
			clock[i] = externalClock[i];
		}
	}

	public void setIndexValue(int id, int value) {
		assert(id<clock.length);
		assert(id>=0);
		clock[id] = value;
	}
 
	@Override
	public String toString(){
		String s = "[ ";
		for( int i= 0; i< this.clock.length; i++){
			s+=  this.clock[i];
			s+=", "; 
		}
		s+= "]";
		return s;}
	
	

}
