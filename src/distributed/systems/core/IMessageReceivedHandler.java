package distributed.systems.core;

import java.net.InetSocketAddress;

import distributed.systems.gridscheduler.model.ControlMessage;
import distributed.systems.gridscheduler.model.SyncLog;

public interface IMessageReceivedHandler {
	public ControlMessage onMessageReceived(Message message);
	public void onExceptionThrown(Message message, InetSocketAddress destinationAddress);
}
