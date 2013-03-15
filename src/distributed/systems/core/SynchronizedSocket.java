package distributed.systems.core;

import java.io.IOException;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import distributed.systems.gridscheduler.model.ControlMessage;

public class SynchronizedSocket extends Thread implements Runnable {
	private ServerSocket socket;
	private IMessageReceivedHandler handler;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	

	public SynchronizedSocket(ServerSocket lSocket) throws IOException {
		socket = lSocket;
	}

	public void sendMessage(ControlMessage controlMessage, String url, int port) {
		
		ClientCom con = new ClientCom(url, port);
        Message outMessage;

        while (!con.open()) {
            try {
                sleep((long) (10));
            } catch (InterruptedException e) {
            }
        }

        outMessage = controlMessage;
        con.writeObject(outMessage);
        con.close();
        
	}

	public void addMessageReceivedHandler(IMessageReceivedHandler handler) {
		assert(handler != null);
		this.handler = handler;
		this.start();
	}

	public void register(String socketURL, int port) {
		// TODO Auto-generated method stub
		try {
			socket.bind(new InetSocketAddress(socketURL,port));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while(true) {
			try {
				Socket s = socket.accept();
				in = new ObjectInputStream(s.getInputStream());
				handler.onMessageReceived((Message)in.readObject());				
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
