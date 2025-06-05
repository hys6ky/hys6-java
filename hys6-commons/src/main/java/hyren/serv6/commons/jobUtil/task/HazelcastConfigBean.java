package hyren.serv6.commons.jobUtil.task;

public class HazelcastConfigBean {

    public static final String CONFNAME = "hazelcast";

    private String localAddress;

    private Integer autoIncrementPort;

    private Integer portCount;

    public String getLocalAddress() {
        return localAddress;
    }

    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    public Integer getAutoIncrementPort() {
        return autoIncrementPort;
    }

    public void setAutoIncrementPort(Integer autoIncrementPort) {
        this.autoIncrementPort = autoIncrementPort;
    }

    public Integer getPortCount() {
        return portCount;
    }

    public void setPortCount(Integer portCount) {
        this.portCount = portCount;
    }
}
