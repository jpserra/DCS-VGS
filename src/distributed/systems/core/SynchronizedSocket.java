package distributed.systems.core;

import distributed.systems.gridscheduler.model.ControlMessage;

import java.net.ServerSocket;

public class SynchronizedSocket extends ServerSocket implements Runnable {
	private ServerSocket socket;
	private IMessageReceivedHandler handler;

	public SynchronizedSocket(ServerSocket lSocket) {
		socket = lSocket;
	}

	public void sendMessage(ControlMessage controlMessage, String string) {
		// TODO Auto-generated method stub
		//socket
		
	}

	public void addMessageReceivedHandler(IMessageReceivedHandler handler) {
		assert(handler == null);
		this.handler = handler;
		this.run();
	}

	public void register(String socketURL) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void run() {
		while(true) {
			socket.
			
		}
	}

}
