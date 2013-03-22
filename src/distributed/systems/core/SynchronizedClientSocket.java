package distributed.systems.core;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import distributed.systems.gridscheduler.model.ControlMessage;
import distributed.systems.gridscheduler.model.SyncLog;


public class SynchronizedClientSocket extends Thread {
	
	private Socket socket;
	private ControlMessage cMessage;
	private IMessageReceivedHandler handler;
	private InetSocketAddress address;
	private boolean requiresRepsonse;
	private SyncLog syncLog;

	
	public SynchronizedClientSocket(ControlMessage cMessage, InetSocketAddress address, IMessageReceivedHandler handler) {
		this.handler = handler;
		socket = new Socket();
		this.cMessage = cMessage;
		this.address = address;
	}

	@Override
	public void run() {
		
		ObjectInputStream in = null;
		ControlMessage msg = null;
		ObjectOutputStream out = null;
		ControlMessage message = null;

		try {
			socket.connect(address);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		try {
			//Send Message
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(cMessage);
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (requiresRepsonse) {
			// Espera pela recepo da resposta at um determinado ponto.
			try {
				socket.setSoTimeout(20000);
				in = new ObjectInputStream(socket.getInputStream());
				msg = (ControlMessage)in.readObject();
				if (syncLog != null ) {
					syncLog.setArrived();
				}

				handler.onMessageReceived(msg);
				in.close();

			} catch (SocketTimeoutException e) {
				System.out.println("Timeout!!!!");
				message = handler.onExceptionThrown(cMessage, address);
				e.printStackTrace();
			} catch (IOException e) {
				message = handler.onExceptionThrown(cMessage, address);
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			out.close();
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if(message != null) sendMessageInSameThread(message);
		
		//TODO Procesar a mensagem... Problemas com concorrencia? Talvez fazer o metodo syncronized
		//handler.onMessageReceived(msg);

	}
	
	public void sendMessage() {
		requiresRepsonse = true;
		Thread t = new Thread(this);
		t.start();
	}
	
	public void sendLogMessage(SyncLog syncLog) {
		this.syncLog = syncLog;
		requiresRepsonse = true;
		Thread t = new Thread(this);
		t.start();
	}

	
	public void sendMessageWithoutResponse() {
		requiresRepsonse = false;
		Thread t = new Thread(this);
		t.start();
	}
	
	public void sendMessageInSameThread(ControlMessage message) {
		this.cMessage = message;
		requiresRepsonse = true;
		this.run();
	}

}
