package distributed.systems.core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

public class LogManager {

	private String filename;
	private int[][] orderedClocks;
	private String[] orderedLogs;

	public int[][] getOrderedClocks() { return orderedClocks; }
	public String[] getOrderedLogs() { return orderedLogs; }
	public LogManager(String filename){ this.filename= filename; }
	public String getFilename() { return filename; }

	public void cleanupStructures() {
		orderedClocks = null;
		orderedLogs = null;
	}

	public synchronized void writeToBinary (Object obj, boolean append){
		File file = new File (filename);
		ObjectOutputStream out = null;

		try{
			if (!file.exists () || !append) 
				out = new ObjectOutputStream (new FileOutputStream (filename));
			else out = new AppendableObjectOutputStream (new FileOutputStream (filename, append));
			out.writeObject(obj);
			out.flush ();
		}catch (Exception e){
			e.printStackTrace ();
		}finally{
			try{
				if (out != null) out.close ();
			}catch (Exception e){
				e.printStackTrace ();
			}
		}
	}

	public ArrayList<LogEntry> readFromBinaryFile (){
		File file = new File (filename);
		ArrayList<LogEntry> recoveredLog = new ArrayList<LogEntry>();
		if (file.exists ()){
			ObjectInputStream ois = null;
			try{
				ois = new ObjectInputStream (new FileInputStream (filename));
				while (true){

					LogEntry j = (LogEntry)ois.readObject ();
					recoveredLog.add(j);
				}
			}catch (EOFException e){

			}catch (Exception e){
				e.printStackTrace ();
			}finally{
				try{
					if (ois != null) ois.close();
				}catch (IOException e){
					e.printStackTrace ();
				}
			}
		}
		return recoveredLog;
	}

	//Gets the Log ordered by clocks
	public void readOrderedLog (){

		cleanupStructures();
		HashMap<int[], String> unorderedLogMap= readLogFromTextfile();

		Set<int[]> clock = unorderedLogMap.keySet();

		int clocks[][] = new int[clock.size()][clock.iterator().next().length];
		int index = 0;
		for(int[] c : clock) {
			clocks[index] = c;
			index++;
		}

		String[] strings = Arrays.asList(unorderedLogMap.values().toArray()).toArray(new String[unorderedLogMap.values().toArray().length]);

		boolean tradeMade, atLeastOne;
		if(clocks.length == 0)
			return;
		int clockLenght = clocks[0].length;

		//VectorialClock tmpClock = null;
		int[] tmpClock = null;
		String aux = null;

		do {
			tradeMade = false;
			for (int i = 0; i < clocks.length - 1; i++) {
				atLeastOne = false;
				for (int j = 0; j < clockLenght; j++) {
					if (clocks[i][j] >= clocks[i + 1][j]) {
						if(clocks[i][j] > clocks[i + 1][j])
							atLeastOne = true;
						if (j == clockLenght-1 && atLeastOne) {
							tmpClock = clocks[i];
							aux = strings[i];
							clocks[i] = clocks[i + 1];
							strings[i]= strings[i+1];
							clocks[i + 1] = tmpClock;
							strings[i+1] = aux;
							tradeMade = true;
						}
					} else {
						j=clockLenght;
					}
				}
			}
		} while (tradeMade);

		orderedClocks = clocks;
		orderedLogs = strings;

	}

	private static class AppendableObjectOutputStream extends ObjectOutputStream {
		public AppendableObjectOutputStream(OutputStream out) throws IOException {
			super(out);
		}


		@Override
		protected void writeStreamHeader() throws IOException {
			reset();
		}
	}

	public synchronized void writeAsText (Object obj, boolean append){
		try {

			File file = new File(filename+".txt");

			FileWriter fw = new FileWriter(file,true);
			BufferedWriter bw = new BufferedWriter(fw);

			bw.write(obj.toString());
			bw.newLine();
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized HashMap<int[], String> readLogFromTextfile (){
		int clock[];
		HashMap<int[], String> log= new HashMap<int[], String>();
		try {
			File file = new File(filename+".txt");
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			while ((line = br.readLine()) != null) {
				// process the line.
				String[] s1 = line.split("]");
				String s3 = s1[0].substring(1);
				String[] s2 = s3.split(",");
				clock = new int[s2.length];
				for(int i = 0; i< s2.length; i++){
					clock[i] = Integer.parseInt(s2[i].trim());
				}
				log.put(clock, line);
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return log;
	}

	/**
	 * The ordered logs must be ready to use. Check readOrderedLog function.
	 * @param extensionToFilename
	 */
	public void writeOrderedLogToTextfile(String extensionToFilename) {
		//readOrderedLog();
		try {
			File file = new File(filename+extensionToFilename);
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			for(String m : orderedLogs) {
				bw.write(m);
				bw.newLine();
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public LogEntryText[] getLogEntriesOrdered() {
		String hostname;
		int port;
		LogEntryType event; 
		int[] clock;
		long jobID = -1;
		LogEntryText[] entries = new LogEntryText[orderedLogs.length];
		int i = 0;
		for(String s : orderedLogs) {
			//Parse string, put values in variables
			String[] s1 = s.split("]");// 0 tem o clock 1 tem o resto
			String[] s2 = s1[1].split(" "); //Event <JobID> Origin
			event = LogEntryType.valueOf(s2[0]);
			if (s2.length == 3){
				jobID = Integer.parseInt(s2[1]);
				String[] s3 = s2[2].split(":");
				hostname= s3[0];
				port = Integer.parseInt(s3[1]);
			}
			else {
				String[] s3 = s2[1].split(":");
				hostname= s3[0];
				port = Integer.parseInt(s3[1]);
			}

			s1[0].replace("[","");
			String[] s4 = s1[0].split(",");
			clock = new int[s4.length];
			for(int j = 0; i< s4.length; i++){
				clock[j] = Integer.parseInt(s4[j].trim());
			}

			//Create instance
			entries[i] = new LogEntryText(hostname, port, event, clock, jobID);
			i++;
		}
		return entries;
	}
}
