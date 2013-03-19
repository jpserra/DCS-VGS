package distributed.systems.core;

import java.net.Socket;

public interface IMessageReceivedHandler {
	public void onMessageReceived(Message message);
}
