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
	private int timeout;

	public SynchronizedClientSocket(ControlMessage cMessage, InetSocketAddress address, IMessageReceivedHandler handler, int timeout) {
		this.handler = handler;
		socket = new Socket();
		this.cMessage = cMessage;
		this.address = address;
		this.timeout = timeout;
	}

	@Override
	public void run() {

		ObjectInputStream in = null;
		ControlMessage msg = null;
		ObjectOutputStream out = null;
		ControlMessage message = null;
		boolean exceptionRequiresResponse = false;

		// Connect the socket
		try {
			socket.connect(address);
		} catch (IOException e) {
			message = handler.onConnectExceptionThrown(cMessage, address, requiresRepsonse);
			exceptionRequiresResponse = requiresRepsonse;
			requiresRepsonse = false;
			e.printStackTrace();
			try {
				socket.close();
			} catch (IOException ex) {
				
			}
		}
		
		// Send the message
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(cMessage);
			out.flush();
		} catch (IOException e) {
			message = handler.onWriteExceptionThrown(cMessage, address, requiresRepsonse);
			exceptionRequiresResponse = requiresRepsonse;
			requiresRepsonse = false;
			e.printStackTrace();
		}
		
		

		// If there is some kind of ACK message, the socket will wait a certain time before throwing an exception.
		if (requiresRepsonse) {
			try {

				socket.setSoTimeout(timeout);
				in = new ObjectInputStream(socket.getInputStream());
				msg = (ControlMessage)in.readObject();

				// In case of a log synchronization, the sender should be awaken after the message.
				if (syncLog != null ) {
					syncLog.setArrived();
				}

				handler.onMessageReceived(msg);
				in.close();

			} catch (SocketTimeoutException e) {
				System.out.println("TIMEOUT! "+cMessage.getType()+
						" from "+cMessage.getUrl()+":"+cMessage.getPort()+
						"to "+address.getHostName()+":"+address.getPort());
				message = handler.onReadExceptionThrown(cMessage, address);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("IOException! "+cMessage.getType()+
						" from "+cMessage.getUrl()+":"+cMessage.getPort()+
						"to "+address.getHostName()+":"+address.getPort());
				message = handler.onReadExceptionThrown(cMessage, address);
				message = handler.onReadExceptionThrown(cMessage, address);
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		else{
			// Give the server time to read the data from the socket
			while(socket.isConnected())
			try {
				Thread.sleep(10);
			}
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		try {
			out.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// In case there is an exception thrown, a new message can be created.
		// If that is the case, that message will be sent in the same thread.
		if(message != null) sendMessageInSameThread(message, exceptionRequiresResponse);

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

	public void sendMessageInSameThread(ControlMessage message, boolean requiresResponse) {
		this.cMessage = message;
		this.requiresRepsonse = requiresResponse;
		this.run();
	}

}
