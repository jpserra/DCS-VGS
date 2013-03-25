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

		// Connect the socket
		try {
			socket.connect(address);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Send the message
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(cMessage);
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// If there is some kind of ACK message, the socket will wait a certain time before throwing an exception.
		if (requiresRepsonse) {
			try {

				socket.setSoTimeout(timeout);
				in = new ObjectInputStream(socket.getInputStream());
				msg = (ControlMessage)in.readObject();

				// In case of a log synchronization, the main thread should be awaken in order to proced.
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
		if(message != null) sendMessageInSameThread(message);

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
