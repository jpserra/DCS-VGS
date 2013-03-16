package distributed.systems.core;

public interface IMessageReceivedHandler {
	public void onMessageReceived(Message message);
}
