package distributed.systems.core;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import distributed.systems.gridscheduler.model.ControlMessage;

public class SynchronizedSocket extends Socket implements Runnable{

//	private Socket socket;
	private ServerSocket serverSocket;
	private IMessageReceivedHandler handler;
	
	public SynchronizedSocket(String localUrl, int localPort) {
		try {
			serverSocket = new ServerSocket(localPort);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void run() {
		while(true) {
			try {
				Socket s = serverSocket.accept();
				//InputStream in = s.getInputStream();
				ObjectInputStream in = new ObjectInputStream(s.getInputStream());
				ControlMessage msg = (ControlMessage)in.readObject();
				System.out.println(msg.getType());
				handler.onMessageReceived(msg);
				in.close();
				s.close();
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}		
	}

	public void addMessageReceivedHandler(IMessageReceivedHandler handler) {
		this.handler = handler;
		Thread t = new Thread(this);
		t.start();
	}

	public void sendMessage(ControlMessage cMessage, InetSocketAddress address) {
		try {
			Socket socket = new Socket(address.getAddress(), address.getPort());
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(cMessage);
			out.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
