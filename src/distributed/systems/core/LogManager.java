package distributed.systems.core;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;

public class LogManager {

	private String filename;

	public LogManager(String filename){
		this.filename= filename;

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
		//teste
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

	//FOR RESOURCE MANAGERS!
	//Gets the Log ordered by clocks
	public LogEntry[] readOrderedLog (){

		ArrayList<LogEntry> unorderedLog = readFromBinaryFile();	
		LogEntry[] orderedLog = new LogEntry[unorderedLog.size()];
		LogEntry tmpLog = null;
		boolean tradeMade, atLeastOne;
		if(unorderedLog.size() == 0)
			return null;
		int clockLenght = (unorderedLog.get(0)).getClock().length;

		//copy
		for (int i = 0; i < unorderedLog.size(); i++) {
			orderedLog[i] = (LogEntry)unorderedLog.get(i);
		}

		do {
			tradeMade = false;
			for (int i = 0; i < orderedLog.length - 1; i++) {
				atLeastOne = false;
				for (int j = 0; j < clockLenght; j++) {
					if (orderedLog[i].getClock()[j] >= orderedLog[i + 1].getClock()[j]) {
						if(orderedLog[i].getClock()[j] > orderedLog[i + 1].getClock()[j])
							atLeastOne = true;
						if (j == clockLenght-1 && atLeastOne) {
							tmpLog = orderedLog[i];
							orderedLog[i] = orderedLog[i + 1];
							orderedLog[i + 1] = tmpLog;
							tradeMade = true;
						}
					} else {
						j=clockLenght;
					}
				}
			}
		} while (tradeMade);

		return orderedLog;
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
	
	public void writeToTextfile() {
		ArrayList<LogEntry> unorderedLog = readFromBinaryFile();
		try {

			File file = new File(filename+"_txt_unordered");

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			for(LogEntry m : unorderedLog) {
				bw.write(m.toString() + "\n");
			}

			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public void writeOrderedToTextfile() {
		// TODO Auto-generated method stub
		LogEntry[] log = readOrderedLog();
		try {

			File file = new File(filename+"_txt");

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			for(LogEntry m : log) {
				bw.write(m.toString() + "\n");
			}

			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
