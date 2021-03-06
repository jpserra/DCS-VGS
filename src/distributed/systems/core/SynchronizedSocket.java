package distributed.systems.core;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import distributed.systems.gridscheduler.model.ControlMessage;

public class SynchronizedSocket extends Thread {

	private ServerSocket serverSocket;
	private IMessageReceivedHandler handler;

	public SynchronizedSocket(String hostname, int port) {
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public class ConnectionHandler extends Thread {
		private Socket s;
		private IMessageReceivedHandler handler;
		public ConnectionHandler(Socket socket, IMessageReceivedHandler handler) {
			s = socket;
			this.handler =  handler;
		}
		
		public void run() {
			
			ObjectOutputStream out;
			ObjectInputStream in;
			
			try {
				
				out = new ObjectOutputStream(s.getOutputStream());
				in = new ObjectInputStream(s.getInputStream());
				
				ControlMessage msg = (ControlMessage)in.readObject();
				ControlMessage replyMsg = handler.onMessageReceived(msg);
				
				if(replyMsg != null) {
					out.writeObject(replyMsg);
					out.flush();
				}
				
				out.close();
				in.close();
				s.close();

			} catch (IOException | ClassNotFoundException e) {
				//TODO Pode falhar aqui?
				e.printStackTrace();
			}

		}
	}

	@Override
	public void run() {
		while(true) {
			try {
				Socket s = serverSocket.accept();
				Thread t = new ConnectionHandler(s, handler);
				t.start();
			} catch (IOException e) {
				System.out.println("#$%#$%#$%#$%#%#$%#%#$%# SERVER SOCKET EXCEPTION... in accept");
				e.printStackTrace();
			}
		}	
	}

	public void addMessageReceivedHandler(IMessageReceivedHandler handler) {
		this.handler = handler;
		Thread t = new Thread( this);
		t.start();
	}
	
}
