package hyren.serv6.hadoop.commons.readConfig;

import org.apache.hadoop.yarn.client.api.YarnClient;

public enum YarnM {

    instance;

    public YarnClient getYarnClient() {
        YarnClient client = YarnClient.createYarnClient();
        client.init(ConfigReader.getConfiguration());
        client.start();
        return client;
    }
}
