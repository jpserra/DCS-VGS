package distributed.systems.core;

import distributed.systems.gridscheduler.model.ControlMessage;

public interface IMessageReceivedHandler {
	public ControlMessage onMessageReceived(Message message);
}
