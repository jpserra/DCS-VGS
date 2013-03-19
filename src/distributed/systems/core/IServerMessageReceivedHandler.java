package distributed.systems.core;

import distributed.systems.gridscheduler.model.ControlMessage;

public interface IServerMessageReceivedHandler {
	public ControlMessage onServerMessageReceived(Message message);
}
