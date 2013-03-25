package distributed.systems.core;

import java.net.InetSocketAddress;
import distributed.systems.gridscheduler.model.ControlMessage;

public interface IMessageReceivedHandler {
	public ControlMessage onMessageReceived(Message message);
	public ControlMessage onConnectExceptionThrown(Message message, InetSocketAddress destinationAddress, boolean requiresRepsonse);
	public ControlMessage onWriteExceptionThrown(Message message, InetSocketAddress destinationAddress, boolean requiresRepsonse);
	public ControlMessage onReadExceptionThrown(Message message, InetSocketAddress destinationAddress);
	
}
