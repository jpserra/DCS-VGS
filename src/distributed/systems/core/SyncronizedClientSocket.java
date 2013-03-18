package distributed.systems.core;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import distributed.systems.gridscheduler.model.ControlMessage;


public class SyncronizedClientSocket extends Thread {
	
	private Socket socket;
	private ControlMessage cMessage;
	
	public SyncronizedClientSocket() {
		this.socket = null;
		this.cMessage = null;
	}

	@Override
	public void run() {
		
		try {
			//Send Message
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(cMessage);
			out.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ObjectInputStream in;
		try {
			socket.setSoTimeout(3000);
			in = new ObjectInputStream(socket.getInputStream());
			ControlMessage msg = (ControlMessage)in.readObject();
			in.close();
		} catch (SocketTimeoutException e) {
			System.out.println("Timeout reached");
			//TODO Fazer alguma coisa em relação à falha;
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//TODO Procesar a mensagem...

	}
	
	public void sendMessage(ControlMessage cMessage, InetSocketAddress address) {
		try {
			socket = new Socket(address.getAddress(), address.getPort());
			this.cMessage = cMessage;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Thread t = new Thread(this);
		t.start();
	}


}
