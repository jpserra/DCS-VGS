package distributed.systems.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;


import distributed.systems.gridscheduler.model.ControlMessage;
import distributed.systems.gridscheduler.model.ControlMessageType;

public class SynchronizedSocket extends Socket implements Runnable{

//	private Socket socket;
	private ServerSocket serverSocket;
	private IMessageReceivedHandler handler;
	
	public SynchronizedSocket(String localUrl, int localPort) {
		try {
			serverSocket = new ServerSocket(localPort);
			Thread t = new Thread(this);
			t.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true) {
			try {
				Socket s = serverSocket.accept();
				//InputStream in = s.getInputStream();
				ObjectInputStream in = new ObjectInputStream(s.getInputStream());
				//String msg = (String)in.readObject();
				ControlMessage msg = (ControlMessage)in.readObject();
				System.out.println(msg.getType());
				handler.onMessageReceived(msg);
				in.close();
				s.close();
			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/*while(true) {
			try {
				System.out.println("SyncSocket loop");

				wait(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}
		*/
		
	}

	public void addMessageReceivedHandler(IMessageReceivedHandler handler) {
		// TODO Auto-generated method stub
		this.handler = handler;
		//Thread t = new Thread(new SynchronizedSocket(socket));
		//t.start(); 
	}

	public void sendMessage(ControlMessage cMessage, InetSocketAddress address) {
		try {
			Socket socket = new Socket(address.getAddress(), address.getPort());
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(cMessage);
			out.close();
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub
		
	}

}
