package distributed.systems.core;

import java.net.InetSocketAddress;
import distributed.systems.gridscheduler.model.ControlMessage;

public interface IMessageReceivedHandler {
	public ControlMessage onMessageReceived(Message message);
	public ControlMessage onExceptionThrown(Message message, InetSocketAddress destinationAddress);
}
