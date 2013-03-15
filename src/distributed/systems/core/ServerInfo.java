package distributed.systems.core;

public class ServerInfo {

    private String hostName;
    private int portNumber;

    public ServerInfo(String hostName, int portNumber) {
        this.hostName = hostName;
        this.portNumber = portNumber;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPortNumber() {
        return portNumber;
    }
}
