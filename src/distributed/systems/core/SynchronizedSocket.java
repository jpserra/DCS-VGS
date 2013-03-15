package distributed.systems.core;

import distributed.systems.gridscheduler.model.ControlMessage;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class SynchronizedSocket extends ServerSocket implements Runnable {
	private ServerSocket socket;
	private IMessageReceivedHandler handler;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	

	public SynchronizedSocket(ServerSocket lSocket) throws IOException {
		socket = lSocket;
	}

	public void sendMessage(ControlMessage controlMessage, String url) {
		//socket
		Socket s;
		try {
			s = new Socket(url, 8080);
			out = new ObjectOutputStream(s.getOutputStream());
			out.writeObject(controlMessage);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void addMessageReceivedHandler(IMessageReceivedHandler handler) {
		assert(handler != null);
		this.handler = handler;
		this.run();
	}

	public void register(String socketURL) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void run() {
		while(true) {
			try {
				Socket s = socket.accept();
				in = new ObjectInputStream(s.getInputStream());
				handler.onMessageReceived((Message)in.readObject());
				//
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

}
