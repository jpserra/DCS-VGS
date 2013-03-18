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
	private IMessageReceivedHandler handler;
	
	public SyncronizedClientSocket(IMessageReceivedHandler handler) {
		this.handler = handler;
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
		ControlMessage msg = null;
		
		// Espera pela recepção da resposta até um determinado ponto.
		try {
			
			socket.setSoTimeout(3000);
			in = new ObjectInputStream(socket.getInputStream());
			msg = (ControlMessage)in.readObject();
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
		
		//TODO Procesar a mensagem... Problemas com concorrencia? Talvez fazer o metodo syncronized
		handler.onMessageReceived(msg);

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
