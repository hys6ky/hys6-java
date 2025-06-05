package hyren.serv6.commons.utils.stream;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class TopicOperator {

    private String topicName = "";

    private String zookeeperHost = "";

    public TopicOperator(String topicName, String zookeeperHost) {
        this.topicName = topicName;
        this.zookeeperHost = zookeeperHost;
    }

    public TopicOperator(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }

    public boolean topicExist() {
        List<String> topicList = getTopicList(zookeeperHost);
        return topicList.contains(topicName);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "zookeeperHost", desc = "", range = "")
    public List<String> getTopicList(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
        Properties adminClientProperties = getAdminClientProperties();
        Set<String> topics = null;
        try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
            ListTopicsResult topicsResult = adminClient.listTopics();
            KafkaFuture<Set<String>> topicsFuture = topicsResult.names();
            topics = topicsFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        List<String> allTopicList = new ArrayList<>(topics);
        return new ArrayList<>(allTopicList);
    }

    public String createTopic(int partitionCount, int replicationFactor) {
        String state = "0";
        Properties adminClientProperties = getAdminClientProperties();
        try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
            log.info("开始创建topic：" + topicName);
            try {
                NewTopic newTopic = new NewTopic(topicName, partitionCount, (short) replicationFactor);
                CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
                result.all().get();
                log.info("创建topic: " + topicName + " 成功，" + "partitionCount: " + partitionCount + " replicationFactor: " + replicationFactor);
            } catch (Exception e) {
                state = "3";
                log.error("创建topic失败！" + e);
            }
        }
        return state;
    }

    public void deleteTopic() {
        Properties adminClientProperties = getAdminClientProperties();
        try (AdminClient adminClient = AdminClient.create(adminClientProperties)) {
            try {
                adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
            } catch (Exception e) {
                log.error("删除topic： " + topicName + "失败！" + e);
            }
        }
    }

    public Properties getAdminClientProperties() {
        Properties properties = new Properties();
        SqlSearchFromKafka sqlSearchFromKafka = new SqlSearchFromKafka();
        List<String> kafkaBrokersByZkHost = sqlSearchFromKafka.getKafkaBrokersByZkHost(this.zookeeperHost);
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokersByZkHost);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 300000);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 300000);
        return properties;
    }
}
